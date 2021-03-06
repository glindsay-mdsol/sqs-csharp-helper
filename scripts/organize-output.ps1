$folderPath = "C:\git\sqs-csharp-helper\src\mdsol.sqs\mdsol.sqs\bin\security\sqs-messages\20130820114217"
$seachRegExRaveVersion = 'com\:mdsol\:'

function Get-URLName ($fileName)
{
    $searchRegExResourceType = [regex]'com:mdsol:([^:]*):'
	$searchRegExEventType = [regex]'"event"\:"([^"]*)"'
    $fileContent = Get-Content $fileName
	
    $resourceType = $searchRegExResourceType.match($fileContent).groups[1].value
	$eventType = $searchRegExEventType.match($fileContent).groups[1].value
	
    return "{0}\{1}" -f $resourceType, $eventType
}

Get-ChildItem $folderPath -Filter *.txt -Recurse | 
    #Where-Object { Get-Content $_.PsPath | Select-String -Pattern $seachRegEx } |
    ForEach-Object {
        $path = Get-URLName($_.FullName)
		$path = ".\{0}" -f $path
		if (!(Test-Path $path)) { New-Item $path -Force -Type Directory }
		Move-Item -Path $_.FullName -Destination $path
        $result = [string]::Format("{1}: {0}", $_.FullName, $path)
        Write-Host $result
    }