$tasks = Get-ScheduledTask | Where-Object { $_.Actions.Execute -like "*stocksage*" -or $_.Actions.Execute -like "*run_chip*" -or $_.Actions.Execute -like "*run_main*" -or $_.Actions.Execute -like "*run_integrity*" }
foreach ($t in $tasks) {
    $info = Get-ScheduledTaskInfo -TaskName $t.TaskName -ErrorAction SilentlyContinue
    Write-Output "$($t.TaskName) | State=$($t.State) | User=$($t.Principal.UserId) | LastRun=$($info.LastRunTime) | Result=$($info.LastTaskResult)"
}
