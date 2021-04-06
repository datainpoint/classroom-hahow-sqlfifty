import pandas as pd
from urllib.parse import quote_plus
from string import ascii_uppercase
import re

class TaiwanElection2020:
    def __init__(self):
        self._counties = ['宜蘭縣', '彰化縣', '金門縣', '桃園市', '苗栗縣', '臺南市', '雲林縣', '南投縣', '高雄市', '臺北市', '新北市', '花蓮縣', '新竹市', '新竹縣', '基隆市', '連江縣', '嘉義縣', '嘉義市', '屏東縣', '澎湖縣', '臺東縣', '臺中市']
    def tidy_dataframe(self, df):
        # updating columns attributes 
        n_cols = df.columns.size
        n_candidates = n_cols - 11
        id_vars = ['town', 'village', 'office']
        candidates = list(df.columns[3:(3 + n_candidates)])
        office_cols = list(ascii_uppercase[:8])
        col_names = id_vars + candidates + office_cols
        df.columns = col_names
        # forward-fill district values
        filled_towns = df['town'].fillna(method='ffill')
        df = df.assign(town=filled_towns)
        # removing summations
        df = df.dropna()
        # removing extra spaces
        stripped_towns = df['town'].str.replace("\u3000", "")
        df = df.assign(town=stripped_towns)
        # pivoting
        df = df.drop(labels=office_cols, axis=1)
        tidy_df = pd.melt(df,
                        id_vars=id_vars,
                        var_name='candidate_info',
                        value_name='votes'
                        )
        return tidy_df
    def adjust_presidential(self, df):
        # split candidate information into 2 columns
        candidate_info_df = df['candidate_info'].str.split("\n", expand=True)
        numbers = candidate_info_df[0].str.replace("\(|\)", "")
        candidates = candidate_info_df[1].str.cat(candidate_info_df[2], sep="/")
        # re-arrange columns
        df = df.drop(labels='candidate_info', axis=1)
        df['number'] = numbers
        df['candidate'] = candidates
        df['office'] = df['office'].astype(int)
        df = df[['county', 'town', 'village', 'office', 'number', 'candidate', 'votes']]
        return df
    def generate_presidential(self):
        presidential = pd.DataFrame()
        for county in self._counties:
            file_name = "總統-A05-4-候選人得票數一覽表-各投開票所({}).xls".format(county)
            file_url = quote_plus(file_name)
            spreadsheet_url = "https://taiwan-election-data.s3-ap-northeast-1.amazonaws.com/presidential_2020/{}".format(file_url)
            # skip those combined cells
            df = pd.read_excel(spreadsheet_url, skiprows=[0, 1, 3, 4], thousands=',')
            tidy_df = self.tidy_dataframe(df)
            # appending dataframe of each city/county
            tidy_df['county'] = county
            presidential = presidential.append(tidy_df)
            print("Tidying {}".format(file_name))
        presidential = presidential.reset_index(drop=True) # reset index for the appended dataframe
        presidential_adjusted = self.adjust_presidential(presidential)
        return presidential_adjusted
    def adjust_legislative(self, df):
        # split candidate information into 2 columns
        candidate_info_df = df['candidate_info'].str.split("\n", expand=True)
        numbers = candidate_info_df[0].str.replace("\(|\)", "")
        candidates = candidate_info_df[1]
        parties = candidate_info_df[2]
        # re-arrange columns
        df = df.drop(labels='candidate_info', axis=1)
        df['number'] = numbers
        df['candidate'] = candidates
        df['party'] = parties
        df['office'] = df['office'].astype(int)
        df['number'] = df['number'].astype(int)
        return df
    def generate_regional(self):
        regional = pd.DataFrame()
        for county in self._counties:
            file_name = "區域立委-A05-2-得票數一覽表({}).xls".format(county)
            file_url = quote_plus(file_name)
            spreadsheet_url = "https://taiwan-election-data.s3-ap-northeast-1.amazonaws.com/legislative_2020/{}".format(file_url)
            xl = pd.ExcelFile(spreadsheet_url)
            for sheet in xl.sheet_names:
                # skip those combined cells
                df = pd.read_excel(spreadsheet_url, skiprows=[0, 1, 3, 4], thousands=',', sheet_name=sheet)
                tidy_df = self.tidy_dataframe(df)
                # appending dataframe of each city/county
                tidy_df['county'] = county
                tidy_df['electoral_district'] = sheet
                regional = regional.append(tidy_df)
                print("Tidying {} of {}".format(sheet, file_name))
        regional = regional.reset_index(drop=True) # reset index for the appended dataframe
        regional_adjusted = self.adjust_legislative(regional)
        regional_adjusted = regional_adjusted[['county', 'electoral_district', 'town', 'village', 'office', 'number', 'party', 'candidate', 'votes']]
        return regional_adjusted
    def generate_indigenous(self):
        indigenous = pd.DataFrame()
        indigenous_types = ['山地', '平地']
        for county in self._counties:
            for indigenous_type in indigenous_types:
                file_name = "{}立委-A05-4-得票數一覽表({}).xls".format(indigenous_type, county)
                file_url = quote_plus(file_name)
                spreadsheet_url = "https://taiwan-election-data.s3-ap-northeast-1.amazonaws.com/legislative_2020/{}".format(file_url)
                # skip those combined cells
                df = pd.read_excel(spreadsheet_url, skiprows=[0, 1, 3, 4], thousands=',')
                tidy_df = self.tidy_dataframe(df)
                # appending dataframe of each city/county
                tidy_df['county'] = county
                tidy_df['electoral_district'] = '{}原住民'.format(indigenous_type)
                indigenous = indigenous.append(tidy_df)
                print("Tidying {}".format(file_name))
        indigenous = indigenous.reset_index(drop=True) # reset index for the appended dataframe
        indigenous_adjusted = self.adjust_legislative(indigenous)
        indigenous_adjusted = indigenous_adjusted[['county', 'electoral_district', 'town', 'village', 'office', 'number', 'party', 'candidate', 'votes']]
        return indigenous_adjusted
    def generate_legislative_regional(self):
        regional = self.generate_regional()
        indigenous = self.generate_indigenous()
        legislative_regional = pd.concat([regional, indigenous], axis=0)
        legislative_regional = legislative_regional.reset_index(drop=True)
        return legislative_regional
    def generate_legislative_at_large(self):
        legislative_at_large = pd.DataFrame()
        for county in self._counties:
            file_name = "不分區立委-A05-6-得票數一覽表({}).xls".format(county)
            file_url = quote_plus(file_name)
            spreadsheet_url = "https://taiwan-election-data.s3-ap-northeast-1.amazonaws.com/legislative_2020/{}".format(file_url)
            # skip those combined cells
            df = pd.read_excel(spreadsheet_url, skiprows=[0, 1, 3, 4], thousands=',')
            tidy_df = self.tidy_dataframe(df)
            # appending dataframe of each city/county
            tidy_df['county'] = county
            legislative_at_large = legislative_at_large.append(tidy_df)
            print("Tidying {}".format(file_name))
        legislative_at_large = legislative_at_large.reset_index(drop=True) # reset index for the appended dataframe
        legislative_at_large_adjusted = self.adjust_legislative(legislative_at_large)
        legislative_at_large_adjusted = legislative_at_large_adjusted[['county', 'town', 'village', 'office', 'number', 'party', 'votes']]
        return legislative_at_large_adjusted
