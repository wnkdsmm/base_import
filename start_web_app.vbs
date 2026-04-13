Option Explicit

Dim shell, fso, projectRoot, appHost, appPort, appUrl, readyUrl, serverCommand
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectRoot = fso.GetParentFolderName(WScript.ScriptFullName)
appHost = "127.0.0.1"
appPort = "8000"
appUrl = "http://" & appHost & ":" & appPort & "/"
readyUrl = "http://" & appHost & ":" & appPort & "/favicon.ico"

If IsUrlReady(readyUrl) Then
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

serverCommand = ResolveServerCommand(projectRoot, appHost, appPort)
If Len(serverCommand) = 0 Then
    MsgBox "Не удалось найти Python или uvicorn для запуска FastAPI." & vbCrLf & _
           "Укажите APP_PYTHON, создайте .venv\Scripts\python.exe или проверьте PATH.", vbExclamation, "Fire Data FastAPI"
    WScript.Quit 1
End If

shell.Run serverCommand, 0, False

If WaitForUrl(readyUrl, 30) Then
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

MsgBox "Сервер не ответил на " & readyUrl & " в течение 30 секунд." & vbCrLf & _
       "Для диагностики запустите start_web_app.cmd.", vbExclamation, "Fire Data FastAPI"
WScript.Quit 1

Function ResolveServerCommand(rootPath, host, port)
    Dim preferredPython, venvPython, localPython
    preferredPython = Trim(shell.ExpandEnvironmentStrings("%APP_PYTHON%"))
    venvPython = rootPath & "\.venv\Scripts\python.exe"

    If Len(preferredPython) > 0 And fso.FileExists(preferredPython) Then
        ResolveServerCommand = BuildShellCommand(rootPath, Quote(preferredPython) & " -m uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    If fso.FileExists(venvPython) Then
        ResolveServerCommand = BuildShellCommand(rootPath, Quote(venvPython) & " -m uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    If CommandExists("uvicorn") Then
        ResolveServerCommand = BuildShellCommand(rootPath, "uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    If CommandExists("py") Then
        ResolveServerCommand = BuildShellCommand(rootPath, "py -m uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    If CommandExists("python") Then
        ResolveServerCommand = BuildShellCommand(rootPath, "python -m uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    localPython = FindPythonInterpreter()
    If Len(localPython) > 0 Then
        ResolveServerCommand = BuildShellCommand(rootPath, Quote(localPython) & " -m uvicorn app.main:app --host " & host & " --port " & port & " --reload")
        Exit Function
    End If

    ResolveServerCommand = ""
End Function

Function FindPythonInterpreter()
    Dim candidate
    candidate = FindPythonInFolder(shell.ExpandEnvironmentStrings("%LocalAppData%") & "\Programs\Python")
    If Len(candidate) > 0 Then
        FindPythonInterpreter = candidate
        Exit Function
    End If

    candidate = FindPythonInFolder(shell.ExpandEnvironmentStrings("%LocalAppData%") & "\Python")
    If Len(candidate) > 0 Then
        FindPythonInterpreter = candidate
        Exit Function
    End If

    FindPythonInterpreter = ""
End Function

Function FindPythonInFolder(parentFolder)
    Dim folder, subFolder, pythonPath, bestName, bestPath
    bestName = ""
    bestPath = ""

    If Not fso.FolderExists(parentFolder) Then
        FindPythonInFolder = ""
        Exit Function
    End If

    Set folder = fso.GetFolder(parentFolder)
    For Each subFolder In folder.SubFolders
        pythonPath = fso.BuildPath(subFolder.Path, "python.exe")
        If fso.FileExists(pythonPath) Then
            If Len(bestName) = 0 Or StrComp(subFolder.Name, bestName, vbTextCompare) > 0 Then
                bestName = subFolder.Name
                bestPath = pythonPath
            End If
        End If
    Next

    FindPythonInFolder = bestPath
End Function

Function BuildShellCommand(rootPath, innerCommand)
    BuildShellCommand = "cmd /c cd /d " & Quote(rootPath) & " && " & innerCommand
End Function

Function Quote(value)
    Quote = Chr(34) & value & Chr(34)
End Function

Function CommandExists(commandName)
    Dim exec
    On Error Resume Next
    Set exec = shell.Exec("cmd /c where " & commandName)
    If Err.Number <> 0 Then
        Err.Clear
        CommandExists = False
        Exit Function
    End If
    exec.StdOut.ReadAll
    exec.StdErr.ReadAll
    CommandExists = (exec.ExitCode = 0)
    On Error GoTo 0
End Function

Function WaitForUrl(url, timeoutSeconds)
    Dim startedAt
    startedAt = Timer
    Do While True
        If IsUrlReady(url) Then
            WaitForUrl = True
            Exit Function
        End If
        If ElapsedSeconds(startedAt) >= timeoutSeconds Then Exit Do
        WScript.Sleep 500
    Loop
    WaitForUrl = False
End Function

Function ElapsedSeconds(startedAt)
    Dim nowValue
    nowValue = Timer
    If nowValue >= startedAt Then
        ElapsedSeconds = nowValue - startedAt
    Else
        ElapsedSeconds = (86400 - startedAt) + nowValue
    End If
End Function

Function IsUrlReady(url)
    Dim request
    On Error Resume Next
    Set request = CreateObject("MSXML2.ServerXMLHTTP.6.0")
    request.setTimeouts 1000, 1000, 2000, 2000
    request.open "GET", url, False
    request.send
    IsUrlReady = (Err.Number = 0 And request.status >= 200 And request.status < 400)
    If Err.Number <> 0 Then Err.Clear
    On Error GoTo 0
End Function
