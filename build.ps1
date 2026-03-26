$excludeFolders = @("venv", ".vscode", "__pycache__") 
$excludeFiles = @("Ifood_opcoes_adicionais.zip", "*.xaml", "*.jproj")

$files = Get-ChildItem -Path . -Recurse | Where-Object {
    -not (
        ($_.PSIsContainer -and ($excludeFolders -contains $_.Name)) -or
        ($_.Name -like $excludeFiles)
    )
}

Compress-Archive -Path $files.FullName -DestinationPath "Ifood_opcoes_adicionais.zip" -Force
