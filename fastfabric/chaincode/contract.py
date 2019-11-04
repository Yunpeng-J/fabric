def execute(rwset, args):
    account1 = args[0]
    account2 = args[1]
    transfer_amount = int(args[2])
    acc1_value = int(rwset[0][account1])
    acc2_value = int(rwset[0][account2])
    if acc1_value - transfer_amount >= 0:
        rwset[1][account1] = str(acc1_value - transfer_amount)
        rwset[1][account2] = str(acc2_value + transfer_amount)

    return rwset
