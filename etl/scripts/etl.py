# -*- coding: utf-8 -*-

import pandas as pd
from ddf_utils.str import to_concept_id

tb_source = '../source/TB_burden_countries.csv'
mdr_source = '../source/MDR_RR_TB_burden_estimates.csv'
ltbi_source = '../source/LTBI_estimates.csv'
dic_source = '../source/TB_data_dictionary.csv'
tb2_source = '../source/TB_burden_age_sex.csv'


def get_indicator_cols(df, other_cols):
    """get indicator columns given the non-indicator columns"""
    cols = list(df.columns)
    for c in other_cols:
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
    tb = pd.read_csv(tb_source)
    mdr = pd.read_csv(mdr_source)
    ltbi = pd.read_csv(ltbi_source)
    dic = pd.read_csv(dic_source)
    tb2 = pd.read_csv(tb2_source)

    # datapoints
    print('generating datapoints...')
    cols = get_indicator_cols(tb, ['country', 'iso2', 'iso_numeric', 'g_whoregion'])
    tb_ = tb[cols].copy()
    tb_['iso3'] = tb_['iso3'].str.lower()
    tb_ = tb_.set_index(['iso3', 'year'])
    create_datapoints(tb_, ['country', 'year'])

    cols = get_indicator_cols(tb2, ['country', 'iso2', 'iso_numeric', 'measure', 'unit'])
    tb2_ = tb2[cols].copy()
    tb2_['iso3'] = tb2_['iso3'].str.lower()
    tb2_['age_group'] = tb2_['age_group'].map(to_concept_id)
    tb2_ = tb2_.rename(columns={'best': 'e_inc_num', 'lo': 'e_inc_num_lo', 'hi': 'e_inc_num_hi'})
    tb2_ = tb2_.set_index(['iso3', 'year', 'age_group', 'sex'])
    create_datapoints(tb2_, ['country', 'year', 'age_group', 'sex'])

    cols = get_indicator_cols(mdr, ['country', 'iso2', 'iso_numeric'])
    mdr_ = mdr[cols].copy()
    mdr_['iso3'] = mdr_['iso3'].str.lower()
    mdr_ = mdr_.set_index(['iso3', 'year'])
    create_datapoints(mdr_, ['country', 'year'])

    cols = get_indicator_cols(ltbi, ['country', 'iso2', 'iso_numeric'])
    ltbi_ = ltbi[cols].copy()
    ltbi_['iso3'] = ltbi_['iso3'].str.lower()
    ltbi_ = ltbi_.set_index(['iso3', 'year'])
    create_datapoints(ltbi_, ['country', 'year'])

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
    country = pd.concat([country1, country2, country3], ignore_index=True)
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

    print('Done.')


if __name__ == '__main__':
    main()
