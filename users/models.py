from datetime import datetime
import uuid
import random
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.models import Model
from pynamodb.attributes import *


def random_number_sixteen_digit():
    return random.randint(1000000000000000, 9999999999999999)


class PackCodeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'pack_code_index'
        read_capacity_units = '4'
        write_capacity_units = '4'
        projection = AllProjection()

    pack_code = UnicodeAttribute(hash_key=True)


# class CodeIndex(GlobalSecondaryIndex):
#     class Meta:
#         index_name = 'code_index'
#         read_capacity_units = '4'
#         write_capacity_units = '4'
#         projection = AllProjection()
#     code = UnicodeAttribute(hash_key=True)


class BrandCodeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'brand_code_index'
        read_capacity_units = '4'
        write_capacity_units = '4'
        projection = AllProjection()

    brand_code = UnicodeAttribute(hash_key=True)
    status = UnicodeAttribute(range_key=True)


class GeneratedCode(Model):

    class Meta:
        table_name = 'dynamo_generated_code'
        region = 'us-west-1'
        aws_access_key_id = 'fakeMyKeyId'
        aws_secret_access_key = 'fakeSecretAccessKey'
        host = "http://localhost:8000"

    brand_code = UnicodeAttribute()
    code = UnicodeAttribute(hash_key=True)
    currency_code = UnicodeAttribute(null=True)
    amount = NumberAttribute(null=True)
    pin = NumberAttribute(null=True)
    pack_code = UnicodeAttribute(null=True)
    app_id = UnicodeAttribute(null=True)
    created_at = UTCDateTimeAttribute()
    exported_at = UTCDateTimeAttribute(null=True)
    status = UnicodeAttribute(default='available')
    brand_code_index = BrandCodeIndex()
    # code_index = CodeIndex()
    pack_code_index = PackCodeIndex()


def create_table():
    if not GeneratedCode.exists():
        print('User Table does not exist proceed into creating it')
        GeneratedCode.create_table(read_capacity_units=10, write_capacity_units=10, wait=True)
    else:
        print('User Table already exists not creating it')


create_table()


# GeneratedCode.delete_table()
# print("deleted table successfully")

def insert_item():
    GeneratedCode(
        brand_code='MYN', code=str(random_number_sixteen_digit()), created_at=datetime.now()
    ).save()
    print('data inserted')


# insert_item()


def insert_n_item_with_pack_code_to_dynamodb(brand_code, n):
    for i in range(1, n):
        GeneratedCode(
            brand_code=brand_code, code=str(random_number_sixteen_digit()),
            pack_code=str(random_number_sixteen_digit()), created_at=datetime.now()
        ).save()
    print("{} item saved".format(i))


# insert_n_item_with_pack_code_to_dynamodb('FLIPKART', 51)


def insert_bulk_item(brand_code, n):
    bulk_items = [GeneratedCode(
        brand_code=brand_code, code=str(random_number_sixteen_digit()), created_at=datetime.now()
    ) for i in range(1, n)]

    with GeneratedCode.batch_write() as batch:
        for item in bulk_items:
            batch.save(item)


# insert_bulk_item('MYN',201)

print("count of data ", GeneratedCode.count())
print(GeneratedCode.brand_code_index.count('AMAZON'))


# Get n limit vouchers for given brand code
def get_n_limit_vouchers_for_given_brand_code(brand_code, limit, currency_code, amount):
    for item in GeneratedCode.brand_code_index.query(brand_code, GeneratedCode.status == 'available', limit=limit):
        print(item.code, item.brand_code, item.currency_code, item.status, item.pack_code)

        # updating the status of brand code to used ,currency_code, amount, exported_at
        item.update(actions=[GeneratedCode.currency_code.set(currency_code),
                             GeneratedCode.amount.set(amount),
                             GeneratedCode.status.set('used'),
                             GeneratedCode.exported_at.set(datetime.now())])
        print("checking status updated or not", item.status)


# get_n_limit_vouchers_for_given_brand_code('FLIPKART', 100, 'INR', 500)


# Get Voucher details from the redemption code
def get_voucher_details_from_redemption_code(code):
    item = GeneratedCode.get(code)
    print("details fetched from redemption code",
          item.code, item.brand_code, item.currency_code, item.amount, item.status, item.exported_at)
    pass


get_voucher_details_from_redemption_code(8921848415522090)


# Get Voucher details from the pack_code
def get_voucher_details_from_pack_code(pack_code):
    for item in GeneratedCode.pack_code_index.query(pack_code):
        print('details fetched from pack code', item.code, item.brand_code, item.pack_code,
              item.currency_code, item.amount, item.status, item.exported_at)


get_voucher_details_from_pack_code(6316543628196884)

#

def get_an_item():
    print('going to fetch item from dynamodb')
    # scan query is very costly don't use
    # for data in GeneratedCode.scan(GeneratedCode.brand_code.startswith('A')):
    #     print(data.id, '-->', data.brand_code)
    for item in GeneratedCode.query('9f7156ce-ae9f-11ed-90d3-4e15793eaa5b'):
        print("query operation ", item.code, item.brand_code, item.created_at)
    print("get operation", GeneratedCode.get('9f7156ce-ae9f-11ed-90d3-4e15793eaa5b').brand_code)


# get_an_item()


def update_an_item():
    generated_code_obj = GeneratedCode.get('9f7156ce-ae9f-11ed-90d3-4e15793eaa5b')
    generated_code_obj.update(actions=[GeneratedCode.created_at.set(datetime.now()),
                                       GeneratedCode.currency_code.set('INR')])
    print("created date in updated function", generated_code_obj.code, generated_code_obj.brand_code,
          generated_code_obj.created_at)


# update_an_item()


# querying secondary index
def get_an_item_from_GSI():
    for brand_code in GeneratedCode.brand_code_index.query('MYN', limit=2):
        print("Item queried from index:", brand_code.code, brand_code.created_at)


# get_an_item_from_GSI()
