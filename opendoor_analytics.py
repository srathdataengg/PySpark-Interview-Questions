"""
Description	All unique checkouts — one row per checkout
customer_id	Customer id
cart_id	Unique cart id
date	Date of the cart checkout

customers:-
Description	All customers, one row per customer
customer_id	Unique customer id
name	Customer name
family_size	Customer family size

checkout_items:-
-------------
Description	All cart-item combinations — each row is an item type in a cart
cart_id	cart id
item	Item in a given cart
quantity	Quantity of item in a cart
price_per_unit_cents	Price per single unit of item in a cart

checkout:-
-------
Description	All unique checkouts — one row per checkout
customer_id	Customer id
cart_id	Unique cart id
date	Date of the cart checkout
"""