using System.Diagnostics;
using Avalonia.Controls;
using Avalonia.Input.Platform;
using NetDownloader.ViewModels;

namespace NetDownloader.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        DataContextChanged += OnDataContextChanged;
        Closing += OnClosing;
    }

    private void OnDataContextChanged(object? sender, EventArgs e)
    {
        if (DataContext is MainViewModel vm)
        {
            vm.AddDownloadRequested -= OnAddDownloadRequested;
            vm.OpenFolderRequested -= OnOpenFolderRequested;
            vm.AddDownloadRequested += OnAddDownloadRequested;
            vm.OpenFolderRequested += OnOpenFolderRequested;
        }
    }

    private async Task<AddDownloadResult?> OnAddDownloadRequested()
    {
        var dialogVm = new AddDownloadViewModel();
        dialogVm.Url = await ReadClipboardTextAsync();

        var dialog = new AddDownloadWindow(dialogVm);
        var result = await dialog.ShowDialog<bool>(this);
        if (!result || !dialogVm.Confirmed)
            return null;

        return new AddDownloadResult
        {
            Url = dialogVm.Url.Trim(),
            SavePath = dialogVm.SavePath.Trim(),
            FileName = string.IsNullOrWhiteSpace(dialogVm.FileName) ? null : dialogVm.FileName.Trim(),
            Connections = dialogVm.Connections,
            StartImmediately = dialogVm.StartImmediately
        };
    }

    /// <summary>
    /// Prefills the Add Download URL field from the system clipboard.
    /// Uses the first non-empty line; trims surrounding whitespace/quotes.
    /// </summary>
    private async Task<string> ReadClipboardTextAsync()
    {
        try
        {
            var clipboard = Clipboard;
            if (clipboard is null)
                return string.Empty;

            var text = await clipboard.TryGetTextAsync();
            if (string.IsNullOrWhiteSpace(text))
                return string.Empty;

            // Prefer first line (browsers / link copy often include trailing newlines).
            var line = text
                .Replace("\r\n", "\n")
                .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .FirstOrDefault() ?? text.Trim();

            return line.Trim().Trim('"', '\'');
        }
        catch
        {
            return string.Empty;
        }
    }

    private static void OnOpenFolderRequested(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = "explorer.exe",
                    Arguments = $"/select,\"{path}\"",
                    UseShellExecute = true
                });
                return;
            }

            var dir = Directory.Exists(path) ? path : Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir) && Directory.Exists(dir))
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = dir,
                    UseShellExecute = true
                });
            }
        }
        catch
        {
            // Ignore OS open failures.
        }
    }

    private void OnClosing(object? sender, WindowClosingEventArgs e)
    {
        if (DataContext is MainViewModel vm)
        {
            vm.AddDownloadRequested -= OnAddDownloadRequested;
            vm.OpenFolderRequested -= OnOpenFolderRequested;
            vm.Dispose();
        }
    }
}
