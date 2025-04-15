import json
import pandas as pd
from db.db.connection import DBConnection


class RawDataProcessor(DBConnection):
    """_summary_

    Args:
        DBConnection (_type_): _description_
    """

    def __init__(self):
        super().__init__()

    def normalize_raw_table(self, source_table, target_table):
        """_summary_

        Args:
            source_table (_type_): _description_
            target_table (_type_): _description_
        """
        try:
            sql = f'''
                SELECT raw_data FROM {source_table}
            '''
            self.execute(sql)
            linhas = self.fetchall()

            todos_df = []

            for (raw_json,) in linhas:
                dados = json.loads(raw_json)

                meta = dados.get("Meta Data", {})
                series = dados.get("Time Series (5min)", {})
                symbol = meta.get("2. Symbol", "N/A")
                timezone = meta.get("6. Time Zone", "UTC")

                if series:
                    df = pd.DataFrame.from_dict(series, orient='index')
                    df.columns = ["open", "high", "low", "close", "volume"]
                    df.index.name = "timestamp"
                    df.reset_index(inplace=True)
                # else:
                    # print(f"Dados de série vazios para o símbolo: {symbol}")
                    # continue

                df[["open", "high", "low", "close"]] = df[[
                    "open", "high", "low", "close"]].astype(float)
                df["volume"] = df["volume"].astype(int)
                df["symbol"] = symbol
                df["timezone"] = timezone

                todos_df.append(df)

            if todos_df:
                final_df = pd.concat(todos_df, ignore_index=True)
                final_df.to_sql(target_table, self.conn,
                                if_exists="replace", index=False)
                print(
                    f"Tabela '{target_table}' criada com {len(final_df)} registros.")
            else:
                print("Nenhum dado bruto encontrado para normalizar.")

        except Exception as e:
            print("Erro ao normalizar dados:", e)
