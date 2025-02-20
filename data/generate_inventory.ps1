# Define the number of rows
$numEntries = 100

# Define start and end date for random timestamps
$startDate = Get-Date "2024-01-01"
$endDate = Get-Date "2024-12-31"

# Sample item names
$items = @("Lanyards", "Acrylic Blanks", "Earrings", "Badge Toppers", "Magnets")

# Create an array to store data
$data = @()

for ($i = 0; $i -lt $numEntries; $i++) {
    # Generate a random timestamp within the range
    $randomTimestamp = Get-Date ($startDate.AddSeconds((Get-Random -Minimum 0 -Maximum ($endDate - $startDate).TotalSeconds))) -Format "yyyy-MM-dd HH:mm:ss"

    # Pick a random item
    $randomItem = $items | Get-Random

    # Generate a random inventory level
    $randomInventory = Get-Random -Minimum 5 -Maximum 500

    # Add data to the array
    $data += [PSCustomObject]@{
        timestamp        = $randomTimestamp
        "Item name"      = $randomItem
        "Inventory level" = $randomInventory
    }
}

# Define output CSV file in the current workspace
$outputPath = "$PSScriptRoot\inventory_data.csv"

# Export to CSV
$data | Export-Csv -Path $outputPath -NoTypeInformation

Write-Host "CSV file created at: $outputPath"
