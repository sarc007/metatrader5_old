import psycopg2
import os
from psycopg2 import Error
from psycopg2.extensions import register_adapter, AsIs
from utils import remove_underscore_add_dash as ruad
def create_part_tbl_spc(*args):
    """
        :argument 0 is connection below
        # connection = psycopg2.connect(user="forex",
        #                               password="Matrix@2020",
        #                               host="127.0.0.1",
        #                               port="5432",
        #                               database="MQLDB")

        :argument 1 is drive the table is supposed to be created
        # drive = 'f:\\'

        :argument 2 currency for which the partition and table space is created
        # currency = 'eurusd'

        :argument 3 date part for which the partition and table space id created
        # date_part = '2020_08'
        :argument 4 who is the owner of the forex db
        # forex_owner = 'forex'

    """
    ret_val = False
    partition_exists = False
    table_space_exists = False
    try:
        connection = args[0]
        cursor = connection.cursor()
        drive = args[1]
        currency = args[2].lower()
        date_part = args[3]
        forex_owner = args[4]
        date_part_str = "'" + date_part + "'"
        tbl_spc = 'forex_' + currency + '_' + date_part
        tbl_spc_str = "'" + tbl_spc + "'"
        tbl_name = currency + '_tbl'
        tbl_name_str = "'"+tbl_name+"'"
        prtn_tbl_name = tbl_name+'_'+date_part
        prtn_tbl_name_str = "'"+prtn_tbl_name+"'"
        location_tbl_spc = drive + 'data\\forex\\'+currency+'\\'+date_part
        location_tbl_spc_str = "'"+location_tbl_spc+"'"

        # creating table space folders
        if os.path.exists(drive+'data') == False:
            os.mkdir(drive+'data')

        if os.path.exists(drive+'data\\forex') == False:
            os.mkdir(drive+'data\\forex')

        if os.path.exists(drive+'data\\forex\\'+currency) == False:
            os.mkdir(drive+'data\\forex\\'+currency)

        if os.path.exists(location_tbl_spc) == False:
            os.mkdir(location_tbl_spc)


        postgreSQL_select_Query = ''' SELECT Count(*) count_value FROM information_schema.tables WHERE table_name =  ''' + prtn_tbl_name_str + ''';'''

        # print(postgreSQL_select_Query)
        cursor.execute(postgreSQL_select_Query)
        # print("Selecting rows from mobile table using cursor.fetchall")
        count_record = cursor.fetchone()

        # print("Print each row and it's columns values")
        # print("Count Record :", count_record[0])
        # print(location_tbl_spc)
        if (count_record[0] == 0):
            # Check for table Space
            postgreSQL_select_Query = ''' SELECT Count(*) count_value FROM pg_tablespace WHERE spcname = ''' + tbl_spc_str + ''';'''
            # print(postgreSQL_select_Query)
            cursor.execute(postgreSQL_select_Query)
            # print("Selecting rows from mobile table using cursor.fetchall")
            count_record = cursor.fetchone()

            # print("Print each row and it's columns values")
            # print("Count Record :", count_record[0])
            # print(location_tbl_spc)
            if (count_record[0] == 0):
                # Create table space
                create_tbl_spc_query = ''' CREATE TABLESPACE  ''' + tbl_spc + '''
                OWNER  ''' + forex_owner + '''
                LOCATION ''' + location_tbl_spc_str + ''';'''
                # print(create_tbl_spc_query)
                cursor.execute(create_tbl_spc_query)
                connection.commit()
                # print("Table created successfully in PostgreSQL ")
                # Create Partition Table
                create_prtn_tbl_query = ''' CREATE TABLE  ''' + prtn_tbl_name + '''
                 partition of   ''' + tbl_name + ''' for values in ('''+ruad(date_part_str)+''')
                 tablespace ''' + tbl_spc + ''';'''
                # print(create_prtn_tbl_query)
                cursor.execute(create_prtn_tbl_query)
                connection.commit()

                # print("Table created successfully in PostgreSQL ")
                ret_val = True
            else:
                ret_val = True
                table_space_exists = True
                return
        else:
            ret_val = True
            partition_exists = True
            return
    except OSError as error:
        print("Error while create folder for table space", error)
        ret_val = False
    except (Exception, psycopg2.Error) as error:
        print("Error while creating PostgreSQL table / table space", error)
        ret_val = False
    finally:
        # closing database connection.
        if not ret_val:
            if (connection):
                cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        if partition_exists:
            print('Partition Table Exists And Table Space Already Exists')
        else:
            print('Partition Table Exists And Table Space Created Successfully')

    return ret_val