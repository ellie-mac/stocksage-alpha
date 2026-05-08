$user = "$env:USERDOMAIN\$env:USERNAME"
$tasks = @(
    "chip_Premarket", "main_Morning", "integrity_Check", "concept_Warm",
    "xhs_Morning", "xhs_Midday", "xhs_Evening", "market_Warm",
    "price_Prefetch", "chip_PerfLog", "main_PerfLog",
    "chip_Night", "main_Scan", "chip_CadScan", "main_Night"
)
foreach ($name in $tasks) {
    $t = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($t) {
        $p = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Highest
        Set-ScheduledTask -TaskName $name -Principal $p -ErrorAction SilentlyContinue | Out-Null
        Write-Output "Updated: $name -> $user"
    } else {
        Write-Output "Not found: $name"
    }
}
