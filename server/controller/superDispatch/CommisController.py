# import os
# from typing import List
# import pandas as pd
# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy.dialects.mysql import insert  # MySQL-specific insert
# from sqlalchemy import update
# from sqlalchemy.future import select
# from model.superDispatch.commisModel import Commis, CommisStats
# from utility.util import XLSX_FILE_PATH

# async def process_commis_xlsx(db: AsyncSession):
#     if not os.path.exists(XLSX_FILE_PATH):
#         print(f"File not found: {XLSX_FILE_PATH}")
#         return

#     df = pd.read_excel(XLSX_FILE_PATH)
#     print(f"Excel columns: {df.columns.tolist()}")

#     store_columns = {
#         1: {
#             "net_sales": "1\nPAS ST-HUBERT\nNet Sales\n10/9/2024",
#             "net_sales_MTD": "1\nPAS ST-HUBERT\nNet Sales MTD",
#         },
#         2: {
#             "net_sales": "2\nPAS ST-JEAN\nNet Sales\n10/9/2024",
#             "net_sales_MTD": "2\nPAS ST-JEAN\nNet Sales MTD",
#         },
#         3: {
#             "net_sales": "3\nPAS CHATEAUGUAY\nNet Sales\n10/9/2024",
#             "net_sales_MTD": "3\nPAS CHATEAUGUAY\nNet Sales MTD",
#         },
#     }

#     for _, row in df.iterrows():
#         if row.get("Counterman") == "Grand Summaries" or any(
#             isinstance(row.get(column), str) and row.get(column).startswith("Sum =")
#             for column in store_columns[1].values()
#         ):
#             print("Skipping summary or grand summary row.")
#             continue

#         for store_id, columns in store_columns.items():
#             net_sales = row.get(columns["net_sales"])
#             net_sales_MTD = row.get(columns["net_sales_MTD"])

#             if pd.isnull(net_sales) and pd.isnull(net_sales_MTD):
#                 print(f"Skipping row for counterman '{row.get('Counterman')}' in store {store_id} due to missing sales data.")
#                 continue

#             st_hubert_summ = float(net_sales_MTD) if store_id == 1 and pd.notnull(net_sales_MTD) else None
#             st_jean_summ = float(net_sales_MTD) if store_id == 2 and pd.notnull(net_sales_MTD) else None
#             chateau_summ = float(net_sales_MTD) if store_id == 3 and pd.notnull(net_sales_MTD) else None

#             commis_data = {
#                 "store": store_id,
#                 "counterman": row.get("Counterman"),
#                 "net_sales": float(net_sales) if pd.notnull(net_sales) else None,
#                 "net_sales_MTD": float(net_sales_MTD) if pd.notnull(net_sales_MTD) else None,
#                 "st_hubert_summ": st_hubert_summ,
#                 "st_jean_summ": st_jean_summ,
#                 "chateau_summ": chateau_summ,
#             }

#             # Check if the record exists
#             stmt = select(CommisStats).where(
#                 CommisStats.store == store_id,
#                 CommisStats.counterman == row.get("Counterman")
#             )
#             result = await db.execute(stmt)
#             existing_record = result.scalar_one_or_none()

#             if existing_record:
#                 # If record exists, update it
#                 update_stmt = (
#                     update(CommisStats)
#                     .where(CommisStats.store == store_id, CommisStats.counterman == row.get("Counterman"))
#                     .values(**commis_data)
#                 )
#                 await db.execute(update_stmt)
#                 print(f"Updated data for counterman '{row.get('Counterman')}' in store {store_id}")
#             else:
#                 # If record does not exist, insert it
#                 insert_stmt = insert(CommisStats).values(**commis_data)
#                 await db.execute(insert_stmt)
#                 print(f"Inserted new data for counterman '{row.get('Counterman')}' in store {store_id}")

#     try:
#         await db.commit()
#         print("All data committed successfully.")
#     except Exception as e:
#         print(f"Error during commit: {e}")

# async def get_all_commis_data(db: AsyncSession) -> List[Commis]:
#     query = select(CommisStats)
#     result = await db.execute(query)
#     commis_records = result.scalars().all()
    
#     # Convert SQLAlchemy models to Pydantic models
#     return [Commis.from_orm(record) for record in commis_records]