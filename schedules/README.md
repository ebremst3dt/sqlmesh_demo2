# How to schedule SQLMesh runs on windows

Note: Run Powershell as an administrator

## View jobs
```
Get-ScheduledJob
```

## View jobs (advanced view)
```
Get-ScheduledJob | Format-List Id, Name, JobTriggers, Command
```

## Register job
```
.\clockwork_daily.ps1
```

## Unregister job
```
Unregister-ScheduledJob -Name clockwork_daily -Force
```