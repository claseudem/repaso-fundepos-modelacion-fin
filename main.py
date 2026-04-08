import yfinance as yf
import pandas as pd
from prefect import flow, task
from pathlib import Path


@task(name="descargar_datos_yahoo_finance")
def descargar_datos_yahoo_finance(tickers: list[str]) -> dict:
    """
    Task que se conecta a Yahoo Finance y descarga datos de los tickers especificados.
    
    Args:
        tickers: Lista de tickers a descargar (ej: ['F', 'TSLA', 'SPY', 'UEC'])
    
    Returns:
        Diccionario con los datos descargados para cada ticker
    """
    datos = {}
    
    for ticker in tickers:
        try:
            print(f"Descargando datos para {ticker}...")
            # Descargar el último año de datos
            df = yf.download(ticker, period='1y', progress=False)
            datos[ticker] = df
            print(f"✓ {ticker}: {len(df)} registros descargados")
        except Exception as e:
            print(f"✗ Error al descargar {ticker}: {str(e)}")
    
    return datos


@task(name="guardar_datos_csv")
def guardar_datos_csv(datos: dict, directorio: str = "data") -> None:
    """
    Task que guarda los datos descargados en archivos CSV.
    
    Args:
        datos: Diccionario con datos de cada ticker
        directorio: Directorio donde guardar los archivos
    """
    path = Path(directorio)
    path.mkdir(exist_ok=True)
    
    for ticker, df in datos.items():
        archivo = path / f"{ticker}.csv"
        df.to_csv(archivo)
        print(f"Archivo guardado: {archivo}")


@flow(name="descarga_datos_financieros")
def flujo_descarga_datos():
    """
    Flow principal que orquesta la descarga de datos de Yahoo Finance.
    """
    tickers = ['F', 'TSLA', 'SPY', 'UEC']
    
    # Ejecutar task de descarga
    datos = descargar_datos_yahoo_finance(tickers)
    
    # Ejecutar task de guardado
    guardar_datos_csv(datos)
    
    print("\n✓ Flow completado exitosamente")


def main():
    """Ejecuta el flow de Prefect."""
    flujo_descarga_datos()


if __name__ == "__main__":
    main()
