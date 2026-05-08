Run the following PowerShell command and display the results as a clean task status table:

```powershell
$today = Get-Date -Format "yyyy/M/d"
Get-ScheduledTask | Where-Object { $_.TaskPath -eq "\\" } | ForEach-Object {
    $info = $_ | Get-ScheduledTaskInfo
    $lastRun = $info.LastRunTime
    $ranToday = $lastRun -and $lastRun.ToString("yyyy/M/d") -eq $today
    $nextRun = $info.NextRunTime
    [PSCustomObject]@{
        Name    = $_.TaskName
        Done    = if ($ranToday) { "✓" } else { " " }
        LastRun = if ($lastRun -and $lastRun.Year -gt 1) { $lastRun.ToString("HH:mm") } else { "--" }
        NextRun = if ($nextRun -and $nextRun.Year -gt 1) { $nextRun.ToString("MM/dd HH:mm") } else { "--" }
        State   = $_.State
    }
} | Sort-Object NextRun | Format-Table -AutoSize
```

Show the output directly. For each task, ✓ means it already ran today, blank means it hasn't. Include NextRun so I can see what's coming up. No commentary needed — just the table.
