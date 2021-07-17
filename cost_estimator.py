

def previous_cost_estimation(sql, db):
    # input: sql
    # output: total execution cost

    cost = db.cost_estimation(sql)

    return cost

def subsequent_cost_estimation(sql, db):

    pass
