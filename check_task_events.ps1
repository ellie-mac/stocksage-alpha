$tasks = @("chip_Premarket", "main_Morning")
foreach ($name in $tasks) {
    Write-Output "=== $name ==="
    Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" -MaxEvents 200 -ErrorAction SilentlyContinue |
        Where-Object { $_.Message -like "*$name*" } |
        Select-Object -First 5 |
        ForEach-Object {
            Write-Output "  $($_.TimeCreated) [$($_.Id)] $($_.Message.Substring(0, [Math]::Min(300, $_.Message.Length)))"
        }
}
