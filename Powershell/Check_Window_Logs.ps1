$Servers = @('server1','server2','server3')

$SearchTerm = "*MSSQLSERVER*"

$DateFilter = (Get-Date).AddDays(-2)

foreach ($Computer in $Servers) {
Get-EventLog -ComputerName $Computer -LogName Application -After $DateFilter -EntryType Error | 
    Where-Object {$_.Source -like $SearchTerm} | 
    Select-Object -Property TimeGenerated, EntryType, Source, Message
    Format-Table
}