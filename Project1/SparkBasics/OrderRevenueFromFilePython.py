from functools import reduce
import sys

order_id = int(sys.argv[1])  # parameter1
order_revenue = open("E:\\Technologies\\ApacheSpark\\data\\retail_db\\order_items\\part-00000", 'r').read().splitlines()
order_revenue_order_id = list(filter(lambda order: int(order.split(",")[1]) == order_id, order_revenue))
order_revenue_price = list(map(lambda price: float(price.split(',')[4]), order_revenue_order_id))
ord_revenue = reduce(lambda x, y: x + y, order_revenue_price)
print(f'Total Revenue from Order Id {order_id} is {ord_revenue}.')
