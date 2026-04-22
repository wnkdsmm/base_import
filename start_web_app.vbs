Option Explicit

Dim shell, fso, projectRoot, logsDir, logFilePath
Dim envFilePath, envExamplePath, envVars
Dim appHost, appPort, appUrl
Dim resolvedPort
Dim venvPython, basePython
Dim bootstrapCommand, startCommand, runCode
Dim reqFilePath, installCode

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
LogMessage "Project root: " & projectRoot

envFilePath = fso.BuildPath(projectRoot, ".env")
envExamplePath = fso.BuildPath(projectRoot, ".env.example")
reqFilePath = fso.BuildPath(projectRoot, "requirements.txt")

If Not fso.FileExists(envFilePath) And fso.FileExists(envExamplePath) Then
    On Error Resume Next
    fso.CopyFile envExamplePath, envFilePath, True
    If Err.Number = 0 Then
        LogMessage ".env created from .env.example"
    Else
        LogMessage "WARN: failed to copy .env.example to .env: " & Err.Description
        Err.Clear
    End If
    On Error GoTo 0
End If

Set envVars = LoadEnvFile(envFilePath)
appHost = GetEnvOrDefault(envVars, "APP_HOST", "127.0.0.1")
appPort = GetEnvOrDefault(envVars, "APP_PORT", "8000")
resolvedPort = ResolveAvailablePort(appPort, 20)
appUrl = "http://" & appHost & ":" & resolvedPort & "/"

LogMessage "Resolved APP_HOST=" & appHost & ", APP_PORT=" & appPort & ", EFFECTIVE_PORT=" & resolvedPort

If IsServerRunning(appUrl) Then
    LogMessage "Server already running. Open browser only."
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

venvPython = fso.BuildPath(projectRoot, ".venv\Scripts\python.exe")
If Not fso.FileExists(venvPython) Then
    basePython = ResolveBasePython()
    If Len(basePython) = 0 Then
        MsgBox "Python not found." & vbCrLf & _
               "Install Python 3.10+ and run setup.bat.", vbCritical, "Fire Data"
        WScript.Quit 1
    End If

    LogMessage ".venv not found. Running one-time bootstrap."
    bootstrapCommand = _
        "cmd /c cd /d " & Quote(projectRoot) & _
        " && " & basePython & " -m venv .venv" & _
        " && .venv\Scripts\python.exe -m pip install --upgrade pip --no-cache-dir" & _
        " && .venv\Scripts\python.exe -m pip install --no-cache-dir -r requirements.txt"

    runCode = shell.Run(bootstrapCommand & " >> " & Quote(logFilePath) & " 2>>&1", 0, True)
    LogMessage "Bootstrap exit code: " & CStr(runCode)
    If runCode <> 0 Then
        MsgBox "Failed to prepare environment." & vbCrLf & _
               "See logs\startup.log for details.", vbCritical, "Fire Data"
        WScript.Quit 1
    End If
End If

If Not fso.FileExists(venvPython) Then
    MsgBox "Python in .venv not found after bootstrap." & vbCrLf & _
           "Run setup.bat manually.", vbCritical, "Fire Data"
    WScript.Quit 1
End If

If Not fso.FileExists(reqFilePath) Then
    MsgBox "requirements.txt not found." & vbCrLf & _
           "Path: " & reqFilePath, vbCritical, "Fire Data"
    WScript.Quit 1
End If

If Not HasRuntimeDeps(venvPython) Then
    LogMessage "Runtime dependencies are missing in .venv. Installing requirements."
    installCode = InstallRequirements(projectRoot, venvPython, reqFilePath, logFilePath)
    LogMessage "Install requirements exit code: " & CStr(installCode)
    If installCode <> 0 Then
        MsgBox "Failed to install Python dependencies." & vbCrLf & _
               "See logs\startup.log for details.", vbCritical, "Fire Data"
        WScript.Quit 1
    End If
Else
    LogMessage "Runtime dependencies in .venv: OK"
End If

startCommand = _
    "cmd /c cd /d " & Quote(projectRoot) & _
    " && " & Quote(venvPython) & " -m uvicorn app.main:app --host " & appHost & " --port " & resolvedPort & " --reload"

LogMessage "Starting server command."
shell.Run startCommand & " >> " & Quote(logFilePath) & " 2>>&1", 0, False

If WaitForUrl(appUrl, 45) Then
    LogMessage "Server ready: " & appUrl
    shell.Run appUrl, 1, False
    WScript.Quit 0
End If

LogMessage "ERROR: Server was not ready in time."
MsgBox "Server did not start in 45 seconds." & vbCrLf & _
       "See logs\startup.log for details.", vbExclamation, "Fire Data"
WScript.Quit 1

Function ResolveBasePython()
    Dim foundPath

    If CommandExists("py") Then
        ResolveBasePython = "py"
        Exit Function
    End If
    If CommandExists("python") Then
        ResolveBasePython = "python"
        Exit Function
    End If

    foundPath = FindPythonInterpreter()
    If Len(foundPath) > 0 Then
        ResolveBasePython = Quote(foundPath)
        Exit Function
    End If

    ResolveBasePython = ""
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

    candidate = FindPythonInFolder(shell.ExpandEnvironmentStrings("%ProgramFiles%") & "\Python")
    If Len(candidate) > 0 Then
        FindPythonInterpreter = candidate
        Exit Function
    End If

    candidate = FindPythonInFolder(shell.ExpandEnvironmentStrings("%ProgramFiles(x86)%") & "\Python")
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

Function ResolveAvailablePort(preferredPort, maxAttempts)
    Dim i, portCandidate
    For i = 0 To maxAttempts
        portCandidate = CStr(CLng(preferredPort) + i)
        If Not IsPortOccupied(portCandidate) Then
            If i > 0 Then
                LogMessage "Preferred port " & preferredPort & " is busy. Using " & portCandidate
            End If
            ResolveAvailablePort = portCandidate
            Exit Function
        End If
    Next
    ResolveAvailablePort = preferredPort
End Function

Function IsPortOccupied(portValue)
    Dim exec, checkCommand
    checkCommand = "cmd /c netstat -ano -p tcp | findstr LISTENING | findstr /c:" & Quote(":" & portValue & " ")
    On Error Resume Next
    Set exec = shell.Exec(checkCommand)
    If Err.Number <> 0 Then
        Err.Clear
        IsPortOccupied = False
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0
    exec.StdOut.ReadAll
    exec.StdErr.ReadAll
    IsPortOccupied = (exec.ExitCode = 0)
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
        Set LoadEnvFile = dict
        Exit Function
    End If

    On Error Resume Next
    Set fileHandle = fso.OpenTextFile(filePath, 1, False)
    If Err.Number <> 0 Then
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

Function HasRuntimeDeps(venvPythonPath)
    Dim exec, checkCommand
    checkCommand = "cmd /c " & Quote(venvPythonPath) & " -c " & Quote("import fastapi,uvicorn,sqlalchemy,pandas; print('deps_ok')")

    On Error Resume Next
    Set exec = shell.Exec(checkCommand)
    If Err.Number <> 0 Then
        Err.Clear
        HasRuntimeDeps = False
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    exec.StdOut.ReadAll
    exec.StdErr.ReadAll
    HasRuntimeDeps = (exec.ExitCode = 0)
End Function

Function InstallRequirements(rootPath, venvPythonPath, requirementsPath, startupLogPath)
    Dim installCommand
    installCommand = _
        "cmd /c cd /d " & Quote(rootPath) & _
        " && " & Quote(venvPythonPath) & " -m pip install --upgrade pip --no-cache-dir" & _
        " && " & Quote(venvPythonPath) & " -m pip install --no-cache-dir -r " & Quote(requirementsPath)
    InstallRequirements = shell.Run(installCommand & " >> " & Quote(startupLogPath) & " 2>>&1", 0, True)
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
