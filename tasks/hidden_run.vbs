' hidden_run.vbs — 隐藏窗口运行 bat 脚本
' 用法: wscript.exe hidden_run.vbs "path\to\script.bat"
' Task Scheduler 调用此脚本可避免弹出控制台窗口
If WScript.Arguments.Count = 0 Then
    WScript.Quit 1
End If
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run Chr(34) & WScript.Arguments(0) & Chr(34), 0, True
