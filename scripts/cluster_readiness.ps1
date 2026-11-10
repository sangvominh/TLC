function Get-Phase2ClusterState {
    $master = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/json/' -TimeoutSec 5
    $nodes = Invoke-RestMethod -Uri 'http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' -TimeoutSec 5
    $info = Invoke-RestMethod -Uri 'http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo' -TimeoutSec 5
    $workers = @($master.workers | Where-Object { $_.state -eq 'ALIVE' }).Count
    $liveNodes = [int]$nodes.beans[0].NumLiveDataNodes
    $safeModeKnown = $info.beans[0].PSObject.Properties.Match('Safemode').Count -gt 0
    $safeModeOff = $safeModeKnown -and [string]::IsNullOrWhiteSpace([string]$info.beans[0].Safemode)
    [pscustomobject]@{
        ready = ($master.status -eq 'ALIVE' -and $workers -ge 1 -and $liveNodes -ge 1 -and $safeModeOff)
        alive_workers = $workers
        live_datanodes = $liveNodes
        hdfs_safemode_off = $safeModeOff
        reason = "master=$($master.status), workers=$workers, datanodes=$liveNodes, safemode_off=$safeModeOff"
    }
}

function Wait-Phase2Cluster {
    param(
        [ValidateRange(1, 900)][int]$TimeoutSeconds = 180,
        [ValidateRange(1, 30)][int]$PollSeconds = 3,
        [string]$LogPath,
        [scriptblock]$Probe = { Get-Phase2ClusterState },
        [scriptblock]$Delay = { param($seconds) Start-Sleep -Seconds $seconds },
        [scriptblock]$Now = { [datetime]::UtcNow }
    )
    $deadline = (& $Now).AddSeconds($TimeoutSeconds)
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    do {
        try { $state = & $Probe }
        catch { $state = [pscustomobject]@{ ready = $false; reason = $_.Exception.Message } }
        if ($LogPath) {
            $record = [ordered]@{ checked_at_utc = (& $Now).ToString('o'); state = $state }
            [System.IO.File]::AppendAllText($LogPath, ($record | ConvertTo-Json -Depth 6 -Compress) + [Environment]::NewLine, $utf8)
        }
        if ($state.ready) {
            Write-Host "READY: $($state.reason)"
            return $state
        }
        if ((& $Now) -ge $deadline) { break }
        Write-Host "Waiting for Spark/HDFS: $($state.reason)"
        & $Delay $PollSeconds
    } while ($true)
    throw "Cluster not ready within $TimeoutSeconds seconds. Last state: $($state.reason). Check Docker containers and http://localhost:8080 / http://localhost:9870."
}
