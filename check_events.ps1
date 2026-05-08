$events = Get-WinEvent -LogName System -MaxEvents 2000 | Where-Object {
    $_.TimeCreated -gt [datetime]'2026-04-24 00:00:00' -and
    $_.TimeCreated -lt [datetime]'2026-04-24 02:30:00'
}
foreach ($e in $events) {
    $msg = $e.Message
    if ($msg.Length -gt 150) { $msg = $msg.Substring(0, 150) }
    Write-Output "$($e.TimeCreated)  [$($e.LevelDisplayName)]  Id=$($e.Id)  $msg"
}
