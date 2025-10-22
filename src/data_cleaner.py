import yfinance as yf
import pandas as pd
import numpy as np

#Con esta clase definimos el objeto de configuración
class DataCleanerConfig:
    def __init__(self, source="yfinance", symbol="QQQ", interval="1h", start_date="2025-10-21", end_date="2025-10-22", csv_path=None):
        self.source = source
        self.symbol = symbol
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.csv_path = csv_path
      

#DEFINIMOS EL CONSTRUCTOR
class DataCleaner:
    def __init__(self, cfg):
            self.cfg = cfg #Esta es la configuración, indica origen, fechas, rutas del csv...
            self.df = None


    def _fetch_yfinance(self):
        #Descarga datos usando yfinance según la configuración dada en DataCleanerConfig
        import warnings
        warnings.filterwarnings("ignore")  # silencia los putos warnings

        data = yf.download(
            tickers=self.cfg.symbol,
            start=self.cfg.start_date,
            end=self.cfg.end_date,
            interval=self.cfg.interval,
            progress=False
        )
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data.columns.name = None

        #peequeño test
        # print("✅ Data descargada:", len(data))
        # print(data)
        # Renombramos las columnas
        data = data.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })

        #Aseguramos que la columna se llame datetime, para que no haaya confusiones
        data = data.reset_index()
        if "Date" in data.columns:
            data = data.rename(columns={"Date": "datetime"})
        elif "Datetime" in data.columns:
            data = data.rename(columns={"Datetime": "datetime"})
        else:
            data["datetime"] = data.index
       
        data.columns = [c.lower() for c in data.columns]
        return data         

    def cargar_datos(self):
        """
        Carga los datos desde la fuente configurada.
        Si la fuente es un CSV, lo lee desde la ruta indicada.
        Si la fuente es yfinance, los descarga.
        Guarda el resultado en self.df y lo devuelve.
        """
        if self.cfg.source == "csv":
            if not self.cfg.csv_path:
                raise ValueError("Debes indicar la ruta del CSV en cfg.csv_path")

            self.df = self._load_csv(self.cfg.csv_path)

        elif self.cfg.source == "yfinance":
            self.df = self._fetch_yfinance()

        else:
            raise ValueError("Fuente de datos no válida. Usa 'csv' o 'yfinance'.")

        return self.df

def preprocess_data(df):

    # Limpieza y preprocesado básico de datos OHLCV.
    # Ordena por fecha
    # Convierte a datetime
    # Sustituye volumen 0 por NaN e interpola
    # Calcula retornos y log-retornos

    df  =  df.copy()

     # Convertimos la columna 'datetime' a tipo date_time y ordenamos los datos por si acasso
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df = df.sort_values("datetime").reset_index(drop=True)

    # Limpiar volumen (solo si existe)
    if "volume" in df.columns:
        df["volume"] = df["volume"].astype(float)
        #Convierte a NaN todas las filas de la columna "volume" donde sea = 0
        df.loc[df["volume"] == 0, "volume"] = np.nan
        #Rellena los datos que NaN interpolando linealmente 
        df["volume"] = df["volume"].interpolate(method="linear")


    # Calcular retornos
    df["return"] = df["close"].pct_change()
    df["log_return"] = np.log(df["close"]).diff()

    # Quitar primeras filas con NaN
    df = df.dropna().reset_index(drop=True)

    return df

# TEST:
cfg = DataCleanerConfig(source="yfinance",symbol="QQQ",interval="1h",start_date="2025-10-21",end_date="2025-10-22")
symbol_df = DataCleaner(cfg)
raw_df = symbol_df.cargar_datos()

#Preprocesar
df_limpio = preprocess_data(raw_df)

print(df_limpio)
print(f"\nFilas finales: {len(df_limpio)}")


# TEST: 
# if __name__ == "__main__":


    # # TEST CARGAR_DATOS
    # cfg = DataCleanerConfig(source="yfinance",symbol="BTC-USD",interval="1h",start_date="2025-10-20", end_date= "2025-10-21")
    # #   TEST CARGAR_DATOS
    # symbol_data = DataCleaner(cfg)
    # datos = symbol_data.cargar_datos()

    # print(datos)
    # print(f"\nFilas descargadas: {len(datos)}")
   

# TEST PREPROCESS_DATA:
   

