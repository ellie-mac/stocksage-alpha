$user = "$env:USERDOMAIN\$env:USERNAME"
$repo = "C:\Users\jiapeichen\repos\stocksage-alpha"

$tasks = @(
    @{ Name="market_Warm";    Time="15:35"; Bat="run_market_warm.bat" },
    @{ Name="price_Prefetch"; Time="15:45"; Bat="run_price_prefetch.bat" }
)

foreach ($t in $tasks) {
    $bat = "$repo\tasks\$($t.Bat)"
    $a   = New-ScheduledTaskAction -Execute $bat
    $tr  = New-ScheduledTaskTrigger -Daily -At $t.Time
    $s   = New-ScheduledTaskSettingsSet -WakeToRun -ExecutionTimeLimit (New-TimeSpan -Hours 2)
    $p   = New-ScheduledTaskPrincipal -UserId $user -LogonType S4U -RunLevel Highest
    Register-ScheduledTask -TaskName $t.Name -Action $a -Trigger $tr -Settings $s -Principal $p -Force | Out-Null
    $info = Get-ScheduledTaskInfo -TaskName $t.Name -ErrorAction SilentlyContinue
    Write-Output "OK: $($t.Name) -> NextRun=$($info.NextRunTime)"
}
