$Servers = @('server1', 'server2', 'server3')

foreach ($Computer in $Servers) {
    Write-Output "`nChecking Driver Space for $Computer...`n"

    Get-CimInstance -ComputerName $Computer -ClassName Win32_LogicalDisk | 
    Select-Object -Property DeviceID, 
        @{Label='FreeSpaceGB'; Expression={ [int]($_.FreeSpace/1GB) }}, 
        @{Label='TotalSpaceGB'; Expression={ [int]($_.Size/1GB) }},
        @{Label='PercentFree'; Expression={ [math]::Round( ([int]($_.FreeSpace/1GB) / [int]($_.Size / 1GB)), 2) }} | 
    Format-Table -AutoSize
}