# -*- coding: utf-8 -*-

import pandas as pd
from ddf_utils.str import to_concept_id

tb_source = '../source/TB_burden_countries.csv'
mdr_source = '../source/MDR_RR_TB_burden_estimates.csv'
ltbi_source = '../source/LTBI_estimates.csv'
dic_source = '../source/TB_data_dictionary.csv'
tb2_source = '../source/TB_burden_age_sex.csv'
noti_source = '../source/TB_notifications.csv'

ignore_cols = ['country', 'iso2', 'iso_numeric', 'measure', 'unit', 'g_whoregion']
ingore_data_dictionary = ["m_01; m_02; etc.", "q_1; q_2; etc."]


def get_indicator_cols(df, other_cols=ignore_cols):
    """get indicator columns given the non-indicator columns"""
    cols = list(df.columns)
    for c in other_cols:
        if c in cols:
            cols.remove(c)
    return cols


def create_datapoints(df_, idx_columns):
    for c in df_:
        df = df_[[c]].dropna().reset_index()
        if df.empty:
            print(f'{c} is empty')
            continue
        cols = idx_columns.copy()
        cols.append(c)
        df.columns = cols
        by = '--'.join(idx_columns)
        df.to_csv(f'../../ddf--datapoints--{c}--by--{by}.csv', index=False)


def main():
    print('reading source files...')
    tb = pd.read_csv(tb_source, na_values=[''], keep_default_na=False)
    mdr = pd.read_csv(mdr_source, na_values=[''], keep_default_na=False)
    ltbi = pd.read_csv(ltbi_source, na_values=[''], keep_default_na=False)
    dic = pd.read_csv(dic_source, na_values=[''], keep_default_na=False)
    dic = dic.loc[~dic['variable_name'].isin(ingore_data_dictionary)]
    tb2 = pd.read_csv(tb2_source, na_values=[''], keep_default_na=False)
    noti = pd.read_csv(noti_source, na_values=[''], keep_default_na=False)

    # datapoints
    print('generating datapoints...')
    cols = get_indicator_cols(tb)
    tb_ = tb[cols].copy()
    tb_['iso3'] = tb_['iso3'].str.lower()
    tb_ = tb_.set_index(['iso3', 'year'])
    create_datapoints(tb_, ['country', 'year'])

    # by age year risk factor
    cols = get_indicator_cols(tb2)
    tb2_ = tb2[cols].copy()
    tb2_['iso3'] = tb2_['iso3'].str.lower()
    tb2_['age_group'] = tb2_['age_group'].map(to_concept_id)
    # rename indicators
    tb2_ = tb2_.rename(columns={'best': 'e_inc_num', 'lo': 'e_inc_num_lo', 'hi': 'e_inc_num_hi'})
    # datapoint for all risk factor
    tb2_1 = tb2_.loc[tb2_['risk_factor'] == 'all'].drop('risk_factor', axis=1)
    tb2_1 = tb2_1.set_index(['iso3', 'year', 'age_group', 'sex'])
    create_datapoints(tb2_1, ['country', 'year', 'age_group', 'sex'])
    # datapoint disaggregated by risk_factor
    tb2_2 = tb2_.loc[tb2_['risk_factor'] != 'all']
    tb2_2 = tb2_2.set_index(['iso3', 'year', 'age_group', 'risk_factor', 'sex'])
    create_datapoints(tb2_2, ['country', 'year', 'age_group', 'risk_factor', 'sex'])

    # MDR/RR
    cols = get_indicator_cols(mdr)
    mdr_ = mdr[cols].copy()
    mdr_['iso3'] = mdr_['iso3'].str.lower()
    mdr_ = mdr_.set_index(['iso3', 'year'])
    create_datapoints(mdr_, ['country', 'year'])

    # latent TB
    cols = get_indicator_cols(ltbi)
    ltbi_ = ltbi[cols].copy()
    ltbi_['iso3'] = ltbi_['iso3'].str.lower()
    ltbi_ = ltbi_.set_index(['iso3', 'year'])
    create_datapoints(ltbi_, ['country', 'year'])

    # notifications
    cols = get_indicator_cols(noti)
    noti_ = noti[cols].copy()
    noti_['iso3'] = noti_['iso3'].str.lower()
    noti_ = noti_.set_index(['iso3', 'year'])
    create_datapoints(noti_, ['country', 'year'])

    # concepts
    cdf = dic[['variable_name', 'definition']].copy()
    cdf.columns = ['concept', 'name']
    cdf['concept_type'] = 'measure'
    cdf['concept'] = cdf['concept'].str.strip()
    cdf.loc[cdf.concept.str.contains('source'), 'concept_type'] = 'string'
    cdf.loc[cdf.concept.str.startswith('iso'), 'concept_type'] = 'string'
    cdf.loc[cdf.concept == 'country', 'concept_type'] = 'entity_domain'
    cdf.loc[cdf.concept == 'age_group', 'concept_type'] = 'entity_domain'
    cdf.loc[cdf.concept == 'sex', 'concept_type'] = 'entity_domain'
    cdf.loc[cdf.concept == 'risk_factor', 'concept_type'] = 'entity_domain'

    cdf_2 = pd.DataFrame([], columns=['concept', 'name', 'concept_type'])
    cdf_2['concept'] = ['name', 'year']
    cdf_2['name'] = ['Name', 'Year']
    cdf_2['concept_type'] = ['string', 'time']

    cdf_ = pd.concat([cdf, cdf_2]).sort_values(by=['concept_type', 'concept'])
    cdf_.to_csv('../../ddf--concepts.csv', index=False)

    # entities
    country1 = tb[['country', 'iso2', 'iso3', 'iso_numeric']]
    country2 = ltbi[['country', 'iso2', 'iso3', 'iso_numeric']]
    country3 = mdr[['country', 'iso2', 'iso3', 'iso_numeric']]
    country4 = tb2[['country', 'iso2', 'iso3', 'iso_numeric']]
    country = pd.concat([country1, country2, country3, country4], ignore_index=True)
    country = (country
               .drop_duplicates()
               .rename(columns={'country': 'name'}))
    country['country'] = country['iso3'].str.lower()
    country = country[['country', 'name', 'iso3', 'iso2', 'iso_numeric']]
    country.sort_values(by=['country']).to_csv('../../ddf--entities--country.csv', index=False)

    age_groups = tb2['age_group'].drop_duplicates()
    age_groups = pd.DataFrame({'age_group': age_groups.map(to_concept_id), 'name': age_groups})
    age_groups.to_csv('../../ddf--entities--age_group.csv', index=False)

    sexs = tb2['sex'].drop_duplicates()
    sexs = pd.DataFrame({'sex': sexs, 'name': ['all', 'female', 'male']})
    sexs.to_csv('../../ddf--entities--sex.csv', index=False)

    # risk factor
    # alc=Harmful use of alcohol; dia=Diabetes; hiv=HIV; smk=Smoking; und=Undernourishment
    risks = tb2[['risk_factor']].drop_duplicates()
    risks = risks[risks['risk_factor'] != 'all']
    risk_names = dict(alc="Harmful use of alcohol",
                      dia="Diabetes",
                      hiv="HIV",
                      smk="Smoking",
                      und="Undernourishment")
    risks['name'] = risks['risk_factor'].map(risk_names)
    risks.to_csv('../../ddf--entities--risk_factor.csv', index=False)

    print('Done.')


if __name__ == '__main__':
    main()
