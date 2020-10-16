import pdb
import pandas as pd
import calendar
from datetime import datetime

branch_wise_sales = {} # stores the total sales tagged with the branch
branch_dish_quantity = {} # stores the number of ordered dishes tagged with the branch

# Parsing and saving the CSV data
print("Reading Input CSV for branch wise data...")
raw_data = pd.read_csv("../consolidated_branch_data/raw_data.csv", encoding='latin1')

# alphabet = string.ascii_letters+string.punctuation
print("Removing blank and invalid rows from input csv...")
raw_data = raw_data.dropna(how = 'any') 
raw_data.drop(raw_data[~raw_data['branch_id'].str.isdigit()].index, inplace = True) # to be fixed - if characters are not found, then breaks here
raw_data["rating"] = raw_data["rating"].astype(int)


# raw_data = raw_data.drop(raw_data["branch_id"].str.isdigit())
print("Reading base price csv...")
base_price_data = pd.read_csv("../consolidated_branch_data/base_price.csv", encoding='latin1').dropna()
base_price_data["Price"] = base_price_data["Price"].astype(int)

def get_sale_month(order_date):
	date_res = datetime.strptime(order_date, "%d-%b-%y")
	return calendar.month_name[date_res.month]

def update_branch_wise_sales(branch_id, amount, order_date):
	month_name = get_sale_month(order_date)
	if branch_wise_sales.get(branch_id):
		sub_keys = branch_wise_sales[branch_id]
		if sub_keys.get(month_name):
			val = sub_keys[month_name]
			branch_wise_sales[branch_id].update({month_name: val + amount})
		else:
			branch_wise_sales[branch_id].update({month_name: amount})
	else:
		branch_wise_sales[branch_id] = {month_name: amount}

def get_ordered_dishes(branch_id, dish_name, quantity):
	if branch_dish_quantity.get(branch_id):
		sub_keys = branch_dish_quantity[branch_id]
		if sub_keys.get(dish_name):
			this_quantity = sub_keys[dish_name]
			branch_dish_quantity[branch_id].update({dish_name: this_quantity + quantity})
		else:
			branch_dish_quantity[branch_id].update({dish_name: quantity})
	else:
		branch_dish_quantity[branch_id] = {dish_name: quantity}

print("Iterating over rows and performing operations...")
for index,row in raw_data.iterrows():
	total_order_amount = 0 # stores the total amount for per order
	order_detail = row["order_details"].split(",")
	for order in order_detail:
		try:
			items = order.split("-")
			item = base_price_data.loc[base_price_data['Item'].str.contains(items[1])] 
			quantity = int(items[0])
			price = item["Price"].values[0]
			total_order_amount = quantity * price
			get_ordered_dishes(row["branch_id"], items[1], quantity)
		except:
			item = base_price_data.loc[base_price_data['Item'].str.contains(order)]
			price = item["Price"].values[0]
			total_order_amount = price
			get_ordered_dishes(row["branch_id"], order, 1)
		update_branch_wise_sales(row["branch_id"], total_order_amount, row["order_date"])
		# ratings_calc(row["branch_id"], row["rating"])

	if index%100 == 0:
		print("Finished operations for " + str(index) + " rows")

print("Writing branch-dish-quantity data to csv for analysis...")
df = pd.DataFrame.from_dict(branch_dish_quantity, orient="index")
df.T.to_csv("../output/branch_dish_quantity.csv")

print("Writing branch-wise-sales data to csv for analysis...")
df = pd.DataFrame.from_dict(branch_wise_sales, orient="index")
df.T.to_csv("../output/branch_wise_sales.csv")

print("Average of ratings per branch...")
print(raw_data.groupby("branch_id")["rating"].mean())

print("Number of ordered dishes in each branch..")
print(branch_dish_quantity)

print("Sales of each branch month wise")
print(branch_wise_sales)
# print(order_details)
# print(raw_data)
