$ErrorActionPreference = "Stop"

$projectRoot = "F:\filesFires\base_import"
$vbsPath = Join-Path $projectRoot "start_web_app.vbs"
$faviconPath = Join-Path $projectRoot "app\static\favicon.ico"

$desktopPath = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktopPath "Fire Analytics.lnk"

if (-not (Test-Path -LiteralPath $vbsPath)) {
    throw "Файл запуска не найден: $vbsPath"
}

$wscriptPath = Join-Path $env:SystemRoot "System32\wscript.exe"
if (-not (Test-Path -LiteralPath $wscriptPath)) {
    throw "wscript.exe не найден: $wscriptPath"
}

$iconLocation = if (Test-Path -LiteralPath $faviconPath) {
    $faviconPath
} else {
    Join-Path $env:SystemRoot "System32\shell32.dll,13"
}

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $wscriptPath
$shortcut.Arguments = '"' + $vbsPath + '"'
$shortcut.WorkingDirectory = $projectRoot
$shortcut.Description = "Fire Analytics Dashboard"
$shortcut.IconLocation = $iconLocation
$shortcut.Save()

Write-Host "Ярлык успешно создан: $shortcutPath"
