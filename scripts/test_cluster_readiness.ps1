$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'cluster_readiness.ps1')

function Assert-ReadyTest($Condition, $Message) {
    if (-not $Condition) { throw $Message }
}

# Exercise the actual readiness predicate, including HDFS safe mode.
$api = @{ workers = 0; datanodes = 1; safemode = '' }
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec)
    if ($Uri -like '*8080*') {
        $workers = @()
        if ($api.workers -gt 0) { $workers = @([pscustomobject]@{ state = 'ALIVE' }) }
        return [pscustomobject]@{ status = 'ALIVE'; workers = $workers }
    }
    if ($Uri -like '*FSNamesystemState*') { return [pscustomobject]@{ beans = @([pscustomobject]@{ NumLiveDataNodes = $api.datanodes }) } }
    return [pscustomobject]@{ beans = @([pscustomobject]@{ Safemode = $api.safemode }) }
}
Assert-ReadyTest (-not (Get-Phase2ClusterState).ready) 'Zero workers must block startup'
$api.workers = 1
$api.datanodes = 0
Assert-ReadyTest (-not (Get-Phase2ClusterState).ready) 'Zero DataNodes must block startup'
$api.datanodes = 1
$api.safemode = 'Safe mode is ON'
Assert-ReadyTest (-not (Get-Phase2ClusterState).ready) 'HDFS safe mode must block startup'
$api.safemode = ''
Assert-ReadyTest ((Get-Phase2ClusterState).ready) 'Healthy services should be ready'
Remove-Item Function:\Invoke-RestMethod

# No real sleeps/services: simulate registration delay and HDFS safe mode.
$clock = @{ time = [datetime]'2026-01-01T00:00:00Z'; attempts = 0; delays = 0 }
$now = { $clock.time }.GetNewClosure()
$delay = { param($seconds) $clock.time = $clock.time.AddSeconds($seconds); $clock.delays++ }.GetNewClosure()
$probe = {
    $clock.attempts++
    [pscustomobject]@{ ready = ($clock.attempts -ge 4); reason = @('worker registering', 'datanode registering', 'HDFS in safe mode', 'all ready')[[math]::Min($clock.attempts - 1, 3)] }
}.GetNewClosure()
$result = Wait-Phase2Cluster -TimeoutSeconds 10 -PollSeconds 1 -Probe $probe -Delay $delay -Now $now
Assert-ReadyTest ($result.ready -and $clock.attempts -eq 4 -and $clock.delays -eq 3) 'Did not wait through service startup'

# HTTP can briefly be unavailable during compose start.
$clock.attempts = 0
$transient = {
    $clock.attempts++
    if ($clock.attempts -eq 1) { throw 'temporary connection refused' }
    [pscustomobject]@{ ready = $true; reason = 'recovered' }
}.GetNewClosure()
$result = Wait-Phase2Cluster -TimeoutSeconds 10 -PollSeconds 1 -Probe $transient -Delay $delay -Now $now
Assert-ReadyTest ($result.ready -and $clock.attempts -eq 2) 'Transient HTTP error did not recover'

# An unavailable cluster must stop after its deadline and preserve its last reason.
$clock.attempts = 0
$neverReady = { $clock.attempts++; [pscustomobject]@{ ready = $false; reason = 'no live datanode' } }.GetNewClosure()
$timedOut = $false
try { Wait-Phase2Cluster -TimeoutSeconds 2 -PollSeconds 1 -Probe $neverReady -Delay $delay -Now $now | Out-Null }
catch { $timedOut = $_.Exception.Message -like '*within 2 seconds*no live datanode*' }
Assert-ReadyTest ($timedOut -and $clock.attempts -eq 3) 'Timeout was not bounded or lost diagnostic state'

Write-Host 'PASSED: delayed registration/safe mode, transient HTTP recovery, bounded timeout.'
