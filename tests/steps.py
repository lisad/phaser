from phaser import batch_step, dataframe_step

#Steps used in tests
@batch_step
def adds_row(batch, context):
    batch.append({'id': 100, 'name': "Fleet Victualling"})
    return batch


@dataframe_step(pass_row_nums=False)
def sum_bonuses(df, context):
    df['total'] = df.sum(axis=1, numeric_only=True)
    return df
