$Servers = @('server1', 'server2', 'server3')

foreach ($Computer in $Servers) {

    Write-Host "`nChecking SQL Services Status for $Computer...`n"

    Get-Service -ComputerName $Computer | Where-Object {$_.DisplayName -like 'SQL Server*'} | Sort-Object -Property 'DisplayName' | 
    ForEach-Object {
        if ($_.Status -eq 'Running') {
            Write-Host -ForegroundColor 'Green' $_.DisplayName 'is running'
        }
        elseif ($_.Status -eq 'Stopped') {
            Write-Host -ForegroundColor 'Red' $_.DisplayName 'is not running'
        }
        else {
            Write-Host -ForegroundColor 'Yellow' $_.DisplayName 'has status: ' $_.Status
        }
    }
}