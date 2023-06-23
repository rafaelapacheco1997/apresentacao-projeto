import pyodbc
import os
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

import warnings
warnings.filterwarnings('ignore')


class DatabaseConnection:
    def __init__(self):
        print("Criando a conexão...")
        current_dir = os.path.realpath(".")
        db_file = "projeto\\01.BASES\\compras2014.mdb"
        db_path = os.path.join(current_dir, db_file)
        self.conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path)
        self.cursor = self.conn.cursor()

    def close_connection(self):
        print("Fechando a conexão ao banco Access...")
        self.conn.close()

    def get_dataframe_from_sql(self, query):
        return pd.read_sql(query, self.conn)


class DataPreprocessing:
    def __init__(self, conn):
        self.conn = conn

    def preprocess_data(self):
        print("Variáveis com as querys...")
        consulta_transacoes = "SELECT * FROM transacoes"
        consulta_itens = "SELECT * FROM itens"
        consulta_itemtransacao = "SELECT * FROM itemtransacao"

        print("Gerando os Dataframes...")
        df_transacoes = self.conn.get_dataframe_from_sql(consulta_transacoes)
        df_itens = self.conn.get_dataframe_from_sql(consulta_itens)
        df_itemtransacao = self.conn.get_dataframe_from_sql(consulta_itemtransacao)

        print("Tratamento das bases...")
        df_itens = df_itens.replace({ 'limao' : 'limão', 'refirgerante' : 'refrigerante', 'Limao' : 'Limão', 'sabao em po' : 'Sabão em pó'})

        for y in ['descrição', 'marca', 'tipo']:
            df_itens[y] = df_itens[y].apply(lambda x: x.title())

        df_itemtransacao = pd.merge(df_itemtransacao, df_itens, how='left', left_on=['item'], right_on=['codItem'])

        df_marca = df_itemtransacao.copy()[['marca']].drop_duplicates()
        df_marca['codMarca'] = range(len(df_marca))

        df_itemtransacao = pd.merge(df_itemtransacao, df_marca, how='left', on=['marca'])

        df = pd.pivot_table(df_itemtransacao,
                            index=['IDTransação'],
                            columns=['descrição'],
                            values=['item'],
                            aggfunc={'item': lambda x: len(x.unique())},
                            fill_value=0)

        df = df.droplevel(0, axis=1)

        df_marca = pd.pivot_table(df_itemtransacao,
                            index=['IDTransação'],
                            columns=['marca'],
                            values=['codMarca'],
                            aggfunc={'codMarca': lambda x: len(x.unique())},
                            fill_value=0)

        df_marca = df_marca.droplevel(0, axis=1)

        return df_transacoes, df_itens, df_itemtransacao, df, df_marca


class AssociationAnalysis:
    def generate_association_rules(self, df):
        print("Regras de Associação...")
        frequent_items = apriori(df, min_support=0.1, use_colnames=True)

        rules = association_rules(frequent_items, metric="confidence", min_threshold=0.5)
        rules = rules[(rules['lift'] > 1) & (rules['zhangs_metric'] > 0.5)]

        apriori_rules = rules
        apriori_rules['lhs_items'] = apriori_rules['antecedents'].apply(lambda x: len(x))
        apriori_rules[apriori_rules['lhs_items'] > 1].sort_values('lift', ascending=False).head()
        apriori_rules['antecedents_'] = apriori_rules['antecedents'].apply(lambda a: ','.join(list(a)))
        apriori_rules['consequents_'] = apriori_rules['consequents'].apply(lambda a: ','.join(list(a)))

        apriori_rules = apriori_rules[['antecedents_', 'consequents_', 'support', 'confidence', 'lift',
                                       'zhangs_metric', 'lhs_items']]

        support_table = pd.pivot_table(apriori_rules,
                                       index='consequents_',
                                       columns='antecedents_',
                                       values='support')

        return apriori_rules, support_table, frequent_items, rules


class DataAnalysis:
    def __init__(self):
        self.db_conn = DatabaseConnection()
        self.data_preprocessing = DataPreprocessing(self.db_conn)
        self.association_analysis = AssociationAnalysis()

    def run_analysis(self):
        df_transacoes, df_itens, df_itemtransacao, df, df_marca = self.data_preprocessing.preprocess_data()
        apriori_rules, support_table, frequent_items, rules = self.association_analysis.generate_association_rules(df)
        self.db_conn.close_connection()
        return df_transacoes, df_itens, df_itemtransacao, df, df_marca, apriori_rules, support_table, frequent_items, rules


print('Chamando as classes...')
data_analysis = DataAnalysis()
df_transacoes, df_itens, df_itemtransacao, df, df_marca, apriori_rules, support_table, frequent_items, rules = data_analysis.run_analysis()

print('Salvando as bases...')

bases = os.path.dirname(os.path.realpath("foo")) + '\\projeto\\01.BASES\\'

df_transacoes.to_excel(bases + 'df_transacoes.xlsx', index=False)
df_itens.to_excel(bases + 'df_itens.xlsx', index=False)
df_itemtransacao.to_excel(bases + 'df_itemtransacao.xlsx', index=False)
apriori_rules.to_excel(bases + 'apriori_rules.xlsx', index=False)
df_marca.to_excel(bases + 'df_marca.xlsx', index=False)