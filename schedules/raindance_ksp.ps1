# Create a trigger that runs daily at 4:00 AM
$trigger = New-JobTrigger -Daily -At "4:00 AM"
$options = New-ScheduledJobOption -RunElevated

Register-ScheduledJob -Name "raindance_ksp_daily" -ScriptBlock {
    # Get current timestamp for file naming
    $job_name = "raindance_ksp_daily"
    $timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
    $logDir = "C:\Users\GAISYSBH2d\sqlmesh_demo2\SQLMeshLogs"
    $logFile = Join-Path -Path $logDir -ChildPath "$job_name $timestamp.txt"

    # Create log directory if it doesn't exist
    if (!(Test-Path -Path $logDir)) {
        New-Item -ItemType Directory -Path $logDir -Force
    }

    # Create initial log entry
    Set-Content -Path $logFile -Value "$timestamp SQLMesh job started."

    $sqlMeshConfigDir = "C:\Users\GAISYSBH2D\sqlmesh_demo2\"
    Set-Location -Path $sqlMeshConfigDir

    # Run SQLMesh command and capture output to the log file
    $output = & sqlmesh --paths "C:\Users\GAISYSBH2D\sqlmesh_demo2\" run --select-model "raindance_ksp" prod 2>&1

    # Append command output to log file
    Add-Content -Path $logFile -Value "Command output:"
    Add-Content -Path $logFile -Value $output

    # Add completion timestamp
    $endTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logFile -Value "$endTime - Job completed"
} -Trigger $trigger -ScheduledJobOption $options