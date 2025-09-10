# Script para ejecutar todo el pipeline de procesamiento ENSU
# Autor: Cascade AI
# Fecha: 2025-09-10

# Verificar si estamos en un entorno virtual y activarlo si es necesario
if (-not (Test-Path variable:global:VIRTUAL_ENV)) {
    Write-Host "Entorno virtual no detectado. Activando..." -ForegroundColor Yellow
    try {
        if (Test-Path ".\.venv\Scripts\Activate.ps1") {
            & ".\.venv\Scripts\Activate.ps1"
        } else {
            Write-Host "No se encontró el entorno virtual. Ejecutando setup.ps1..." -ForegroundColor Yellow
            & .\setup.ps1
        }
    } catch {
        Write-Host "Error al activar el entorno virtual. Continuando de todos modos..." -ForegroundColor Red
    }
}

# Crear directorios necesarios si no existen
$directories = @(
    ".\data\yucatan-inseguridad\insumos\raw",
    ".\data\yucatan-inseguridad\insumos\diccionarios",
    ".\data\yucatan-inseguridad\procesados",
    ".\data\yucatan-inseguridad\reportes",
    ".\data\yucatan-inseguridad\logs"
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        Write-Host "Creando directorio: $dir" -ForegroundColor Green
        New-Item -Path $dir -ItemType Directory -Force | Out-Null
    }
}

# Definir colores para salida
$colorInfo = "Cyan"
$colorSuccess = "Green"
$colorError = "Red"
$colorHeader = "Magenta"

# Función para mostrar cabeceras
function Show-Header {
    param (
        [string]$Title
    )
    
    Write-Host
    Write-Host ("-" * 70) -ForegroundColor $colorHeader
    Write-Host $Title -ForegroundColor $colorHeader
    Write-Host ("-" * 70) -ForegroundColor $colorHeader
}

# Función para ejecutar comandos y manejar errores
function Execute-Command {
    param (
        [string]$Title,
        [string]$Command,
        [string]$ErrorMessage = "Ocurrió un error al ejecutar el comando"
    )
    
    Show-Header $Title
    
    try {
        Invoke-Expression $Command
        if ($LASTEXITCODE -ne 0) {
            throw "El comando finalizó con código de salida: $LASTEXITCODE"
        }
        Write-Host "`n✓ Ejecución exitosa" -ForegroundColor $colorSuccess
    }
    catch {
        Write-Host "`n✗ $ErrorMessage" -ForegroundColor $colorError
        Write-Host $_.Exception.Message -ForegroundColor $colorError
        Write-Host "`n¿Desea continuar con el resto del pipeline? (s/n)" -ForegroundColor $colorInfo
        $continue = Read-Host
        
        if ($continue -ne "s") {
            Write-Host "Abortando pipeline..." -ForegroundColor $colorError
            exit 1
        }
    }
}

# Ejecución del pipeline completo
Write-Host
Write-Host "======================================================" -ForegroundColor $colorHeader
Write-Host "    PIPELINE DE PROCESAMIENTO ENSU - YUCATÁN" -ForegroundColor $colorHeader
Write-Host "======================================================" -ForegroundColor $colorHeader
Write-Host "Fecha: $(Get-Date -Format 'dd/MM/yyyy HH:mm:ss')" -ForegroundColor $colorInfo
Write-Host

# Paso 1: Orquestador
Execute-Command -Title "1. Orquestador: Descubriendo y catalogando datos ENSU" -Command {
    python .\orquestador_ensu.py --root ".\datos completos"
} -ErrorMessage "Error en el proceso de orquestación"

# Paso 2: Procesador
Execute-Command -Title "2. Procesador: Generando agregados por municipio y periodo" -Command {
    python .\procesar_ensu_cb.py
} -ErrorMessage "Error en el procesamiento de datos"

# Paso 3: Generador de reporte
Execute-Command -Title "3. Generador de reporte: Creando visualizaciones" -Command {
    python .\generar_reporte_evolucion.py
} -ErrorMessage "Error en la generación del reporte"

# Paso 4: Control de calidad
Execute-Command -Title "4. Control de calidad: Verificando resultados" -Command {
    python .\qa_checks.py
} -ErrorMessage "Advertencia: Se detectaron problemas en el control de calidad"

# Resultados
Show-Header "RESULTADOS DEL PIPELINE"

# Verificar si existen los archivos principales
$archivos_salida = @{
    "Inventario" = ".\data\yucatan-inseguridad\logs\inventario_ensu.json"
    "Resumen" = ".\data\yucatan-inseguridad\logs\resumen_procesamiento.json"
    "Reporte" = ".\data\yucatan-inseguridad\reportes\reporte_yucatan_evolucion.html"
}

foreach ($key in $archivos_salida.Keys) {
    $path = $archivos_salida[$key]
    
    if (Test-Path $path) {
        Write-Host "$key generado exitosamente:" -ForegroundColor $colorSuccess
        Write-Host "  $path" -ForegroundColor $colorInfo
    } else {
        Write-Host "$key no encontrado:" -ForegroundColor $colorError
        Write-Host "  $path" -ForegroundColor $colorError
    }
}

Write-Host
Write-Host "Pipeline completado." -ForegroundColor $colorSuccess
Write-Host "Para ver el reporte, abra:" -ForegroundColor $colorInfo
Write-Host "  $($archivos_salida['Reporte'])" -ForegroundColor $colorInfo
Write-Host

# Ofrecer abrir el reporte automáticamente
Write-Host "¿Desea abrir el reporte HTML ahora? (s/n)" -ForegroundColor $colorInfo
$openReport = Read-Host

if ($openReport -eq "s") {
    if (Test-Path $archivos_salida['Reporte']) {
        Start-Process $archivos_salida['Reporte']
        Write-Host "Reporte abierto en el navegador predeterminado." -ForegroundColor $colorSuccess
    } else {
        Write-Host "No se pudo abrir el reporte porque no existe." -ForegroundColor $colorError
    }
}
