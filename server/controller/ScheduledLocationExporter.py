# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# import pytz
# from fastapi import FastAPI
# import logging

# # Import your existing classes and utilities
# from controller.OfflineInv import EmailConfig, LocationExporter
# from utility.util import PrimarySessionLocal

# logger = logging.getLogger(__name__)

# class ScheduledLocationExporter:
#     """Handles scheduled exports of location data for multiple stores"""
    
#     def __init__(self, app: FastAPI, email_config: EmailConfig):
#         self.app = app
#         self.scheduler = AsyncIOScheduler()
#         self.location_exporter = LocationExporter(email_config)

#     async def export_store(self, store_id: str):
#         """Export location data for a specific store"""
#         try:
#             logger.info(f"Starting scheduled export for store {store_id}")
            
#             # Use your existing session factory
#             async with PrimarySessionLocal() as session:
#                 try:
#                     # Use your existing export_locations_by_email method
#                     result = await self.location_exporter.export_locations_by_email(
#                         store_id, 
#                         session
#                     )
#                     logger.info(f"Store {store_id} export completed: {result}")
#                 except Exception as e:
#                     logger.error(f"Export operation failed: {str(e)}", exc_info=True)
#                     await session.rollback()
#                     raise
                
#         except Exception as e:
#             logger.error(f"Failed to export store {store_id}: {str(e)}", exc_info=True)

#     def schedule_exports(self):
#         """Schedule individual exports for each store"""
#         eastern_tz = pytz.timezone('America/New_York')
        
#         # Schedule separate jobs for each store
#         stores = ["1", "2", "3"]
#         for store_id in stores:
#             self.scheduler.add_job(
#                 self.export_store,
#                 'cron',
#                 args=[store_id],
#                 minute='*/10',    # Every 10 minutes
#                 hour='7-19',      # From 7 AM to 7 PM (19:00)
#                 timezone=eastern_tz,
#                 id=f'store_{store_id}_export',
#                 name=f'Export locations for store {store_id}',
#                 replace_existing=True
#             )
#             logger.info(f"Scheduled export job for store {store_id}")
        
#         self.scheduler.start()
#         logger.info("All store exports scheduled for every 10 minutes between 7 AM and 7 PM Eastern")

# def setup_scheduled_exports(app: FastAPI, email_config: EmailConfig):
#     """Set up scheduled exports"""
#     exporter = ScheduledLocationExporter(app, email_config)
#     exporter.schedule_exports()
    
#     @app.on_event("shutdown")
#     async def shutdown_scheduler():
#         exporter.scheduler.shutdown()