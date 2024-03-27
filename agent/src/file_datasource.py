from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None
        self.accelerometer_reader = None
        self.gps_reader = None
        self.parking_reader = None

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        accelerometer_data = next(self.accelerometer_reader, None)
        gps_data = next(self.gps_reader, None)
        parking_data = next(self.parking_reader, None)

        if accelerometer_data is None:
            self.accelerometer_file.seek(0)
            next(self.accelerometer_reader, None)
            accelerometer_data = next(reader(self.accelerometer_file), None)

        if gps_data is None:
            self.gps_file.seek(0)
            next(self.gps_reader, None)
            gps_data = next(reader(self.gps_file), None)

        if parking_data is None:
            self.parking_file.seek(0)
            next(self.parking_reader, None)
            parking_data = next(reader(self.parking_file), None)

        if accelerometer_data is not None:
            accelerometer_data = [int(x) for x in accelerometer_data]
        if gps_data is not None:
            gps_data = [float(x) for x in gps_data]
        if parking_data is not None:
            parking_data = int(parking_data[0])



        return AggregatedData(
            Accelerometer(*accelerometer_data),
            Gps(*gps_data),
            # Parking(parking_data, Gps(*gps_data)),
            datetime.now(),
            config.USER_ID,
        )

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file = open(self.accelerometer_filename, "r")
        self.gps_file = open(self.gps_filename, "r")
        self.parking_file = open(self.parking_filename, "r")

        self.accelerometer_reader = reader(self.accelerometer_file)
        self.gps_reader = reader(self.gps_file)
        self.parking_reader = reader(self.parking_file)

        next(self.accelerometer_reader)
        next(self.gps_reader)
        next(self.parking_reader)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.accelerometer_file.close()
        if self.parking_file:
            self.parking_file.close()
