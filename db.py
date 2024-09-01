import psycopg2
import csv

# Define your database credentials directly in the code
DBNAME="Delivery_final_project"
USER="postgres"
PASSWORD="5859"
HOST="localhost"
PORT="5432"

# Establish connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
cur = conn.cursor()

def load_csv_to_db(table_name, csv_file_path, columns):
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row from CSV
        for row in reader:
            print(f"Processing row: {row}")  # Debugging line
            print(f"Expected columns: {len(columns)}, Row length: {len(row)}")  # Debugging line
            if len(row) != len(columns):
                print(f"Row length mismatch, skipping: {row}")  # Debugging line
                continue
            cur.execute(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                row
            )
    conn.commit()


# Paths to your CSV files and corresponding tables
csv_files = {
    'food': {
        'path': 'cleaned_food.csv',
        'columns': ['f_id', 'item', 'veg_or_non_veg']
    },
    'menu': {
        'path': 'cleaned_menu_df.csv',
        'columns': ['id', 'menu_id', 'r_id', 'f_id', 'price', 'cuisine_1', 'cuisine_2']
    },
    'orders': {
        'path': 'cleaned_orders_df.csv',
        'columns': ['order_id', 'order_date', 'sales_qty', 'sales_amount', 'currency', 'user_id', 'r_id']
    },
    'restaurant': {
        'path': 'cleaned_restaurant_df.csv',
        'columns': ['id', 'name', 'city', 'rating', 'rating_count', 'avg_cost_for_a_meal', 'cuisine_1', 'cuisine_2']
    },
    'users': {
        'path': 'cleaned_users_df.csv',
        'columns': ['user_id', 'name', 'age', 'gender', 'marital_status', 'occupation', 'monthly_income', 'educational_qualifications', 'family_size']
    }
}

# Iterate through the CSV files and load them into the respective tables
for table_name, file_info in csv_files.items():
    load_csv_to_db(table_name, file_info['path'], file_info['columns'])

# Close the cursor and connection
cur.close()
conn.close()
