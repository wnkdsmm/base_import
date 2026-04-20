Option Explicit

' ============================================================
' start_web_app.vbs
' Changes:
' 1) Added .env parsing (line-by-line key=value).
' 2) Added PostgreSQL availability check using DATABASE_URL.
' 3) Added "already running" detection: open browser only, no restart.
' 4) Added startup logging to logs\startup.log.
' 5) Tray icon note: native tray icon is not supported by pure WScript/VBScript.
' ============================================================

Dim shell, fso, projectRoot, logsDir, logFilePath
Dim appHost, appPort, appUrl, readyUrl
Dim envVars, envFilePath
Dim serverCommand, runCommand, pythonForChecks, dbUrl

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectRoot = fso.GetParentFolderName(WScript.ScriptFullName)
logsDir = fso.BuildPath(projectRoot, "logs")
If Not fso.FolderExists(logsDir) Then
    On Error Resume Next
    fso.CreateFolder logsDir
    On Error GoTo 0
End If
logFilePath = fso.BuildPath(logsDir, "startup.log")

LogMessage "==== startup begin ===="
LogMessage "Script path: " & WScript.ScriptFullName
LogMessage "Project root: " & projectRoot

envFilePath = fso.BuildPath(projectRoot, ".env")
Set envVars = LoadEnvFile(envFilePath)

appHost = GetEnvOrDefault(envVars, "APP_HOST", "127.0.0.1")
appPort = GetEnvOrDefault(envVars, "APP_PORT", "8000")
appUrl = "http://" & appHost & ":" & appPort & "/"
readyUrl = appUrl

LogMessage "Resolved APP_HOST=" & appHost & ", APP_PORT=" & appPort

' Changed: if server is already responding, just open browser and quit.
If IsServerRunning(readyUrl) Then
    LogMessage "Server already running at " & readyUrl & ". Browser will be opened."
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

' Resolve server command (kept unchanged by requirement).
serverCommand = ResolveServerCommand(projectRoot, appHost, appPort)
If Len(serverCommand) = 0 Then
    LogMessage "ERROR: Cannot resolve server command (python/uvicorn not found)."
    MsgBox "Cannot find Python/uvicorn for FastAPI startup." & vbCrLf & _
           "Set APP_PYTHON, create .venv\Scripts\python.exe, or check PATH.", vbExclamation, "Fire Data FastAPI"
    WScript.Quit 1
End If

' Changed: check PostgreSQL availability before startup.
dbUrl = GetEnvOrDefault(envVars, "DATABASE_URL", "")
If Len(dbUrl) = 0 Then
    LogMessage "ERROR: DATABASE_URL is missing in .env."
    MsgBox "DATABASE_URL is not set in .env." & vbCrLf & _
           "Add DATABASE_URL=postgresql://user:password@host:5432/dbname and retry.", vbExclamation, "Fire Data FastAPI"
    WScript.Quit 1
End If

pythonForChecks = ResolvePythonForChecks(projectRoot)
If Len(pythonForChecks) = 0 Then
    LogMessage "ERROR: Cannot resolve python interpreter for database check."
    MsgBox "Cannot find Python interpreter for PostgreSQL pre-check." & vbCrLf & _
           "Ensure .venv\Scripts\python.exe exists or set APP_PYTHON.", vbExclamation, "Fire Data FastAPI"
    WScript.Quit 1
End If

If Not IsDatabaseReachable(pythonForChecks, dbUrl) Then
    LogMessage "ERROR: PostgreSQL is not reachable for DATABASE_URL from .env."
    MsgBox "PostgreSQL is not available." & vbCrLf & _
           "Check DATABASE_URL in .env and ensure PostgreSQL is running." & vbCrLf & vbCrLf & _
           "Quick checklist:" & vbCrLf & _
           "1. Start PostgreSQL service." & vbCrLf & _
           "2. Verify host/port/user/password/db in DATABASE_URL." & vbCrLf & _
           "3. Test network/firewall access to DB host:port." & vbCrLf & vbCrLf & _
           "Details are written to logs\startup.log.", vbCritical, "Fire Data FastAPI"
    WScript.Quit 1
End If

LogMessage "PostgreSQL pre-check: OK"

' Changed: append server stdout/stderr to startup log.
runCommand = serverCommand & " >> " & Quote(logFilePath) & " 2>>&1"
LogMessage "Starting server command: " & serverCommand
shell.Run runCommand, 0, False

If WaitForUrl(readyUrl, 30) Then
    LogMessage "Server is ready: " & readyUrl
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

LogMessage "ERROR: Server did not become ready within timeout."
MsgBox "Server did not respond at " & readyUrl & " within 30 seconds." & vbCrLf & _
       "Run start_web_app.cmd for interactive diagnostics." & vbCrLf & _
       "See logs\startup.log for details.", vbExclamation, "Fire Data FastAPI"
WScript.Quit 1

' NOTE about tray icon:
' Pure WScript/VBScript does not provide a native API to keep a tray icon process.
' Implementing tray icon would require HTA/PowerShell/.NET helpers, which are outside current constraints.

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

Function ResolvePythonForChecks(rootPath)
    Dim preferredPython, venvPython
    preferredPython = Trim(shell.ExpandEnvironmentStrings("%APP_PYTHON%"))
    venvPython = rootPath & "\.venv\Scripts\python.exe"

    If Len(preferredPython) > 0 And fso.FileExists(preferredPython) Then
        ResolvePythonForChecks = preferredPython
        Exit Function
    End If

    If fso.FileExists(venvPython) Then
        ResolvePythonForChecks = venvPython
        Exit Function
    End If

    If CommandExists("py") Then
        ResolvePythonForChecks = "py"
        Exit Function
    End If

    If CommandExists("python") Then
        ResolvePythonForChecks = "python"
        Exit Function
    End If

    ResolvePythonForChecks = FindPythonInterpreter()
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
        If IsServerRunning(url) Then
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

Function IsServerRunning(url)
    Dim request, statusCode
    On Error Resume Next
    Set request = CreateObject("MSXML2.ServerXMLHTTP.6.0")
    request.setTimeouts 1000, 1000, 2000, 2000
    request.open "GET", url, False
    request.send
    statusCode = 0
    If Err.Number = 0 Then statusCode = request.status
    IsServerRunning = (Err.Number = 0 And statusCode >= 200 And statusCode < 500)
    If Err.Number <> 0 Then Err.Clear
    On Error GoTo 0
End Function

Function LoadEnvFile(filePath)
    Dim dict, fileHandle, lineText, eqPos, keyName, valueText
    Set dict = CreateObject("Scripting.Dictionary")

    If Not fso.FileExists(filePath) Then
        LogMessage "WARN: .env file not found at " & filePath
        Set LoadEnvFile = dict
        Exit Function
    End If

    On Error Resume Next
    Set fileHandle = fso.OpenTextFile(filePath, 1, False)
    If Err.Number <> 0 Then
        LogMessage "ERROR: Failed to open .env file: " & Err.Description
        Err.Clear
        On Error GoTo 0
        Set LoadEnvFile = dict
        Exit Function
    End If
    On Error GoTo 0

    Do Until fileHandle.AtEndOfStream
        lineText = Trim(fileHandle.ReadLine)
        If Len(lineText) = 0 Then
            ' skip
        ElseIf Left(lineText, 1) = "#" Then
            ' skip
        Else
            eqPos = InStr(1, lineText, "=", vbTextCompare)
            If eqPos > 1 Then
                keyName = Trim(Left(lineText, eqPos - 1))
                valueText = Trim(Mid(lineText, eqPos + 1))
                valueText = StripWrappingQuotes(valueText)
                If Len(keyName) > 0 Then
                    If dict.Exists(keyName) Then
                        dict(keyName) = valueText
                    Else
                        dict.Add keyName, valueText
                    End If
                End If
            End If
        End If
    Loop
    fileHandle.Close
    Set LoadEnvFile = dict
End Function

Function StripWrappingQuotes(valueText)
    Dim outValue
    outValue = valueText
    If Len(outValue) >= 2 Then
        If (Left(outValue, 1) = Chr(34) And Right(outValue, 1) = Chr(34)) Or (Left(outValue, 1) = "'" And Right(outValue, 1) = "'") Then
            outValue = Mid(outValue, 2, Len(outValue) - 2)
        End If
    End If
    StripWrappingQuotes = outValue
End Function

Function GetEnvOrDefault(envDict, keyName, defaultValue)
    If Not envDict Is Nothing Then
        If envDict.Exists(keyName) Then
            GetEnvOrDefault = envDict(keyName)
            Exit Function
        End If
    End If
    GetEnvOrDefault = defaultValue
End Function

Function BuildDatabaseCheckCommand(pythonCmd, dbUrlValue)
    Dim scriptText
    scriptText = _
        "import socket,sys;" & _
        "from urllib.parse import urlparse;" & _
        "u=urlparse(sys.argv[1]);" & _
        "h=u.hostname or '';" & _
        "p=u.port or 5432;" & _
        "if not h: sys.exit(2);" & _
        "s=socket.socket(socket.AF_INET,socket.SOCK_STREAM);s.settimeout(3);" & _
        "s.connect((h,p));s.close();print('db_ok',h,p)"
    BuildDatabaseCheckCommand = "cmd /c " & Quote(pythonCmd) & " -c " & Quote(scriptText) & " " & Quote(dbUrlValue)
End Function

Function IsDatabaseReachable(pythonCmd, dbUrlValue)
    Dim exec, commandText, stderrText, stdoutText
    commandText = BuildDatabaseCheckCommand(pythonCmd, dbUrlValue)
    LogMessage "DB check command prepared with python: " & pythonCmd

    On Error Resume Next
    Set exec = shell.Exec(commandText)
    If Err.Number <> 0 Then
        LogMessage "ERROR: Failed to execute DB check command: " & Err.Description
        Err.Clear
        IsDatabaseReachable = False
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    stdoutText = exec.StdOut.ReadAll
    stderrText = exec.StdErr.ReadAll
    If Len(Trim(stdoutText)) > 0 Then LogMessage "DB check stdout: " & Replace(stdoutText, vbCrLf, " | ")
    If Len(Trim(stderrText)) > 0 Then LogMessage "DB check stderr: " & Replace(stderrText, vbCrLf, " | ")
    LogMessage "DB check exit code: " & CStr(exec.ExitCode)

    IsDatabaseReachable = (exec.ExitCode = 0)
End Function

Sub LogMessage(messageText)
    Dim fileHandle, lineText
    lineText = FormatDateTime(Now, 2) & " " & FormatDateTime(Now, 4) & " | " & messageText
    On Error Resume Next
    Set fileHandle = fso.OpenTextFile(logFilePath, 8, True)
    If Err.Number = 0 Then
        fileHandle.WriteLine lineText
        fileHandle.Close
    Else
        Err.Clear
    End If
    On Error GoTo 0
End Sub
