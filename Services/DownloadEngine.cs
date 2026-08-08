using System.Diagnostics;
using System.Net.Http.Headers;
using NetDownloader.Models;

namespace NetDownloader.Services;

/// <summary>
/// Multi-connection HTTP downloader with Range-based resume support.
/// Partial content is written to a .part file; segment offsets are tracked for pause/resume.
/// </summary>
public sealed class DownloadEngine
{
    private const int SpeedSampleIntervalMilliseconds = 750;
    private const double SpeedSmoothingFactor = 0.35;

    private static readonly HttpClient SharedHttp = CreateHttpClient();

    private readonly DownloadItem _item;
    private CancellationTokenSource? _cts;
    private readonly object _writeLock = new();
    private FileStream? _fileStream;
    private long _sessionBytes;
    private readonly Stopwatch _speedWatch = new();
    private long _lastSpeedSampleBytes;
    private double _currentSpeed;

    public event Action<DownloadItem, double>? ProgressChanged;
    public event Action<DownloadItem>? StatusChanged;

    public DownloadEngine(DownloadItem item)
    {
        _item = item;
    }

    public double CurrentSpeedBytesPerSec => _currentSpeed;

    private static HttpClient CreateHttpClient()
    {
        var handler = new HttpClientHandler
        {
            AllowAutoRedirect = true,
            MaxAutomaticRedirections = 10
        };
        var client = new HttpClient(handler)
        {
            Timeout = Timeout.InfiniteTimeSpan
        };
        client.DefaultRequestHeaders.UserAgent.ParseAdd(
            "NetDownloader/1.0 (Avalonia; +https://github.com/local/NetDownloader)");
        return client;
    }

    public async Task StartAsync(CancellationToken externalToken = default)
    {
        if (_item.Status == DownloadStatus.Completed)
            return;

        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        var token = _cts.Token;

        try
        {
            SetStatus(DownloadStatus.Connecting);

            await ProbeAndPrepareAsync(token).ConfigureAwait(false);

            EnsureTempFile();
            OpenFileStream();

            SetStatus(DownloadStatus.Downloading);
            _sessionBytes = 0;
            _speedWatch.Restart();
            _lastSpeedSampleBytes = _item.DownloadedBytes;

            if (_item.SupportsResume && _item.Segments.Count > 1)
            {
                await DownloadMultiSegmentAsync(token).ConfigureAwait(false);
            }
            else
            {
                await DownloadSingleStreamAsync(token).ConfigureAwait(false);
            }

            token.ThrowIfCancellationRequested();

            CloseFileStream();
            FinalizeFile();
            _item.DownloadedBytes = _item.TotalBytes > 0 ? _item.TotalBytes : _item.DownloadedBytes;
            _item.CompletedAt = DateTime.Now;
            SetStatus(DownloadStatus.Completed);
            RaiseProgress();
        }
        catch (OperationCanceledException)
        {
            CloseFileStream();
            if (_item.Status != DownloadStatus.Paused && _item.Status != DownloadStatus.Cancelled)
                SetStatus(DownloadStatus.Paused);
            RaiseProgress();
        }
        catch (Exception ex)
        {
            CloseFileStream();
            _item.ErrorMessage = ex.Message;
            SetStatus(DownloadStatus.Failed);
            RaiseProgress();
        }
        finally
        {
            _speedWatch.Stop();
            _cts?.Dispose();
            _cts = null;
        }
    }

    public void Pause()
    {
        if (_item.Status is not (DownloadStatus.Downloading or DownloadStatus.Connecting or DownloadStatus.Queued))
            return;

        SetStatus(DownloadStatus.Paused);
        try { _cts?.Cancel(); } catch { /* ignore */ }
    }

    public void Cancel()
    {
        SetStatus(DownloadStatus.Cancelled);
        try { _cts?.Cancel(); } catch { /* ignore */ }
    }

    private async Task ProbeAndPrepareAsync(CancellationToken token)
    {
        // Fresh start or incomplete metadata: probe server.
        if (_item.TotalBytes <= 0 || _item.Segments.Count == 0)
        {
            using var request = new HttpRequestMessage(HttpMethod.Head, _item.Url);
            using var response = await SendWithGetFallbackAsync(request, token).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            _item.TotalBytes = response.Content.Headers.ContentLength ?? 0;
            _item.ETag = response.Headers.ETag?.Tag;
            if (response.Content.Headers.LastModified is { } lm)
                _item.LastModified = lm.ToString("R");

            var acceptRanges = response.Headers.AcceptRanges;
            _item.SupportsResume = acceptRanges.Any(r =>
                r.Equals("bytes", StringComparison.OrdinalIgnoreCase));

            // Some servers omit Accept-Ranges on HEAD; try a tiny range GET.
            if (!_item.SupportsResume && _item.TotalBytes > 0)
            {
                _item.SupportsResume = await ProbeRangeSupportAsync(token).ConfigureAwait(false);
            }

            if (string.IsNullOrWhiteSpace(_item.FileName))
                _item.FileName = ResolveFileName(response);

            EnsurePaths();
            InitSegments();
        }
        else
        {
            EnsurePaths();
            // Existing job: keep segments; optionally re-check that the resource still matches.
            if (_item.SupportsResume && _item.DownloadedBytes > 0)
            {
                var stillValid = await ValidateRemoteAsync(token).ConfigureAwait(false);
                if (!stillValid)
                {
                    // Resource changed — restart cleanly.
                    ResetPartialData();
                    InitSegments();
                }
            }

            if (_item.Segments.Count == 0)
                InitSegments();
        }

        // Recalculate downloaded from segments / temp file.
        if (_item.Segments.Count > 0)
            _item.DownloadedBytes = _item.Segments.Sum(s => s.Downloaded);
    }

    private async Task<HttpResponseMessage> SendWithGetFallbackAsync(
        HttpRequestMessage headRequest, CancellationToken token)
    {
        try
        {
            var headResponse = await SharedHttp.SendAsync(
                headRequest, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

            // Some hosts reject HEAD.
            if ((int)headResponse.StatusCode is 405 or 501 or 403)
            {
                headResponse.Dispose();
                using var getRequest = new HttpRequestMessage(HttpMethod.Get, _item.Url);
                getRequest.Headers.Range = new RangeHeaderValue(0, 0);
                return await SharedHttp.SendAsync(
                    getRequest, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            }

            return headResponse;
        }
        catch
        {
            using var getRequest = new HttpRequestMessage(HttpMethod.Get, _item.Url);
            getRequest.Headers.Range = new RangeHeaderValue(0, 0);
            return await SharedHttp.SendAsync(
                getRequest, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        }
    }

    private async Task<bool> ProbeRangeSupportAsync(CancellationToken token)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, _item.Url);
            request.Headers.Range = new RangeHeaderValue(0, 0);
            using var response = await SharedHttp.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            return response.StatusCode == System.Net.HttpStatusCode.PartialContent ||
                   response.Content.Headers.ContentRange is not null;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> ValidateRemoteAsync(CancellationToken token)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Head, _item.Url);
            using var response = await SharedHttp.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return true; // Don't wipe data on transient probe failure.

            var length = response.Content.Headers.ContentLength;
            if (length is > 0 && _item.TotalBytes > 0 && length != _item.TotalBytes)
                return false;

            var etag = response.Headers.ETag?.Tag;
            if (!string.IsNullOrEmpty(_item.ETag) && !string.IsNullOrEmpty(etag) &&
                !string.Equals(_item.ETag, etag, StringComparison.Ordinal))
                return false;

            return true;
        }
        catch
        {
            return true;
        }
    }

    private void InitSegments()
    {
        _item.Segments.Clear();

        if (!_item.SupportsResume || _item.TotalBytes <= 0)
        {
            // Single stream, unknown or no range.
            _item.ConnectionCount = 1;
            _item.Segments.Add(new DownloadSegment
            {
                Index = 0,
                Start = 0,
                End = _item.TotalBytes > 0 ? _item.TotalBytes - 1 : long.MaxValue - 1,
                Downloaded = 0
            });
            return;
        }

        var connections = Math.Clamp(_item.ConnectionCount, 1, 16);
        // Small files: one connection is enough.
        if (_item.TotalBytes < 1 * 1024 * 1024)
            connections = 1;

        _item.ConnectionCount = connections;
        var chunk = _item.TotalBytes / connections;

        for (var i = 0; i < connections; i++)
        {
            var start = i * chunk;
            var end = i == connections - 1 ? _item.TotalBytes - 1 : start + chunk - 1;
            _item.Segments.Add(new DownloadSegment
            {
                Index = i,
                Start = start,
                End = end,
                Downloaded = 0
            });
        }
    }

    private void ResetPartialData()
    {
        CloseFileStream();
        if (File.Exists(_item.TempFilePath))
        {
            try { File.Delete(_item.TempFilePath); } catch { /* ignore */ }
        }

        _item.DownloadedBytes = 0;
        _item.Segments.Clear();
        _item.ErrorMessage = null;
    }

    private void EnsurePaths()
    {
        if (string.IsNullOrWhiteSpace(_item.SavePath))
        {
            _item.SavePath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                "Downloads");
        }

        Directory.CreateDirectory(_item.SavePath);

        if (string.IsNullOrWhiteSpace(_item.FileName))
            _item.FileName = "download.bin";

        // Sanitize file name.
        foreach (var c in Path.GetInvalidFileNameChars())
            _item.FileName = _item.FileName.Replace(c, '_');

        if (string.IsNullOrWhiteSpace(_item.TempFilePath))
        {
            _item.TempFilePath = Path.Combine(_item.SavePath, _item.FileName + ".part");
        }
    }

    private string ResolveFileName(HttpResponseMessage response)
    {
        var cd = response.Content.Headers.ContentDisposition;
        if (!string.IsNullOrWhiteSpace(cd?.FileNameStar))
            return cd.FileNameStar.Trim('"');
        if (!string.IsNullOrWhiteSpace(cd?.FileName))
            return cd.FileName.Trim('"');

        try
        {
            var uri = new Uri(_item.Url);
            var name = Path.GetFileName(uri.LocalPath);
            if (!string.IsNullOrWhiteSpace(name))
                return Uri.UnescapeDataString(name);
        }
        catch { /* ignore */ }

        return $"download_{_item.Id:N}.bin";
    }

    private void EnsureTempFile()
    {
        EnsurePaths();
        if (!File.Exists(_item.TempFilePath))
        {
            using var fs = new FileStream(
                _item.TempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            if (_item.TotalBytes > 0)
            {
                // Pre-allocate when size is known (sparse-friendly on NTFS).
                try { fs.SetLength(_item.TotalBytes); } catch { /* ignore if not supported */ }
            }
        }
        else if (_item.TotalBytes > 0)
        {
            try
            {
                using var fs = new FileStream(
                    _item.TempFilePath, FileMode.Open, FileAccess.Write, FileShare.None);
                if (fs.Length < _item.TotalBytes)
                    fs.SetLength(_item.TotalBytes);
            }
            catch { /* ignore */ }
        }
    }

    private void OpenFileStream()
    {
        _fileStream = new FileStream(
            _item.TempFilePath,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.Read,
            bufferSize: 1024 * 128,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
    }

    private void CloseFileStream()
    {
        try
        {
            _fileStream?.Flush(true);
            _fileStream?.Dispose();
        }
        catch { /* ignore */ }
        finally
        {
            _fileStream = null;
        }
    }

    private void FinalizeFile()
    {
        var finalPath = _item.FinalFilePath;
        if (File.Exists(finalPath))
        {
            var dir = Path.GetDirectoryName(finalPath)!;
            var name = Path.GetFileNameWithoutExtension(finalPath);
            var ext = Path.GetExtension(finalPath);
            var i = 1;
            string candidate;
            do
            {
                candidate = Path.Combine(dir, $"{name} ({i}){ext}");
                i++;
            } while (File.Exists(candidate));
            finalPath = candidate;
            _item.FileName = Path.GetFileName(finalPath);
        }

        File.Move(_item.TempFilePath, finalPath, overwrite: false);
        _item.TempFilePath = string.Empty;
    }

    private async Task DownloadMultiSegmentAsync(CancellationToken token)
    {
        var tasks = _item.Segments
            .Where(s => !s.IsComplete)
            .Select(s => DownloadSegmentAsync(s, token))
            .ToArray();

        if (tasks.Length == 0)
            return;

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task DownloadSegmentAsync(DownloadSegment segment, CancellationToken token)
    {
        while (!segment.IsComplete)
        {
            token.ThrowIfCancellationRequested();

            var from = segment.Start + segment.Downloaded;
            var to = segment.End;

            using var request = new HttpRequestMessage(HttpMethod.Get, _item.Url);
            request.Headers.Range = new RangeHeaderValue(from, to);

            using var response = await SharedHttp.SendAsync(
                request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

            if (response.StatusCode is not (
                System.Net.HttpStatusCode.PartialContent or System.Net.HttpStatusCode.OK))
            {
                response.EnsureSuccessStatusCode();
            }

            // If server ignored Range and returned full body, fall back carefully.
            if (response.StatusCode == System.Net.HttpStatusCode.OK &&
                response.Content.Headers.ContentRange is null &&
                from > 0)
            {
                throw new InvalidOperationException(
                    "Server stopped supporting range requests; cannot resume multi-segment download.");
            }

            await using var stream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
            var buffer = new byte[1024 * 64];

            while (segment.Downloaded < segment.Length)
            {
                token.ThrowIfCancellationRequested();

                var toRead = (int)Math.Min(buffer.Length, segment.Remaining);
                var read = await stream.ReadAsync(buffer.AsMemory(0, toRead), token).ConfigureAwait(false);
                if (read == 0)
                    break;

                var writePos = segment.Start + segment.Downloaded;
                await WriteAtAsync(writePos, buffer, read, token).ConfigureAwait(false);

                segment.Downloaded += read;
                Interlocked.Add(ref _sessionBytes, read);
                UpdateTotalsAndProgress();
            }

            // If connection dropped mid-segment, loop continues and re-requests remaining range.
            if (!segment.IsComplete)
            {
                await Task.Delay(500, token).ConfigureAwait(false);
            }
        }
    }

    private async Task DownloadSingleStreamAsync(CancellationToken token)
    {
        var segment = _item.Segments[0];
        var from = segment.Downloaded;

        using var request = new HttpRequestMessage(HttpMethod.Get, _item.Url);
        if (_item.SupportsResume && from > 0)
            request.Headers.Range = new RangeHeaderValue(from, null);

        using var response = await SharedHttp.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

        if (_item.SupportsResume && from > 0 &&
            response.StatusCode == System.Net.HttpStatusCode.OK &&
            response.Content.Headers.ContentRange is null)
        {
            // Server ignored Range — must restart from zero.
            segment.Downloaded = 0;
            _item.DownloadedBytes = 0;
            from = 0;
        }
        else
        {
            response.EnsureSuccessStatusCode();
        }

        if (_item.TotalBytes <= 0 && response.Content.Headers.ContentLength is { } len)
        {
            // If we resumed, ContentLength is remaining; adjust.
            _item.TotalBytes = from > 0 && response.StatusCode == System.Net.HttpStatusCode.PartialContent
                ? from + len
                : len;
            segment.End = _item.TotalBytes - 1;
            if (_fileStream is not null && _fileStream.Length < _item.TotalBytes)
            {
                try { _fileStream.SetLength(_item.TotalBytes); } catch { /* ignore */ }
            }
        }

        await using var stream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        var buffer = new byte[1024 * 64];

        while (true)
        {
            token.ThrowIfCancellationRequested();
            var read = await stream.ReadAsync(buffer, token).ConfigureAwait(false);
            if (read == 0)
                break;

            var writePos = segment.Downloaded;
            await WriteAtAsync(writePos, buffer, read, token).ConfigureAwait(false);
            segment.Downloaded += read;
            Interlocked.Add(ref _sessionBytes, read);
            UpdateTotalsAndProgress();
        }

        if (_item.TotalBytes <= 0)
            _item.TotalBytes = segment.Downloaded;
    }

    private async Task WriteAtAsync(long position, byte[] buffer, int count, CancellationToken token)
    {
        // Serialize writes to the shared temp file.
        var stream = _fileStream ?? throw new InvalidOperationException("File stream is not open.");
        // FileStream is not fully thread-safe for concurrent Seek+Write; lock.
        // Use Task.Run-free approach with Monitor.
        await Task.Run(() =>
        {
            lock (_writeLock)
            {
                stream.Position = position;
                stream.Write(buffer, 0, count);
            }
        }, token).ConfigureAwait(false);
    }

    private void UpdateTotalsAndProgress()
    {
        _item.DownloadedBytes = _item.Segments.Sum(s => s.Downloaded);

        // Refresh at a readable cadence and smooth short-lived network bursts so the
        // displayed speed and ETA remain stable instead of jumping on every buffer write.
        if (_speedWatch.ElapsedMilliseconds >= SpeedSampleIntervalMilliseconds)
        {
            var elapsed = _speedWatch.Elapsed.TotalSeconds;
            if (elapsed > 0)
            {
                var delta = _item.DownloadedBytes - _lastSpeedSampleBytes;
                var instantaneousSpeed = delta / elapsed;
                _currentSpeed = _currentSpeed <= 0
                    ? instantaneousSpeed
                    : _currentSpeed + (instantaneousSpeed - _currentSpeed) * SpeedSmoothingFactor;
                _lastSpeedSampleBytes = _item.DownloadedBytes;
                _speedWatch.Restart();

                RaiseProgress();
            }
        }
    }

    private void RaiseProgress() => ProgressChanged?.Invoke(_item, _currentSpeed);

    private void SetStatus(DownloadStatus status)
    {
        if (status != DownloadStatus.Downloading)
            _currentSpeed = 0;

        _item.Status = status;
        StatusChanged?.Invoke(_item);
    }
}
