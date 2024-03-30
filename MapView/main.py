import asyncio
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
from lineMapLayer import LineMapLayer
from datasource import Datasource


class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__()
        self.car_marker = None
        self.pothole_marker = None
        self.bump_marker = None
        self.map_layer = LineMapLayer()
        self.mapview = None
        self.datasource = Datasource(1)
        self.car_route_points = []
        # додати необхідні змінні

    def on_start(self):
        """
        Встановлює необхідні маркери, викликає функцію для оновлення мапи
        """
        self.mapview.add_layer(self.map_layer, mode="scatter")
        Clock.schedule_interval(self.update, 1)  # оновлення кожну секунду

    def update(self, *args):
        """
        Викликається регулярно для оновлення мапи
        """
        new_points = self.datasource.get_new_points()
        for point in new_points:
            if point[2]:
                self.update_car_marker((point[0], point[1]))
                self.add_to_car_route((point[0], point[1]))
            if point[2] == 'pothole':
                self.set_pothole_marker((point[0], point[1]))
            if point[2] == 'bump':
                self.set_bump_marker((point[0], point[1]))


    def update_car_marker(self, point):
        """
        Оновлює відображення маркера машини на мапі
        :param point: GPS координати
        """
        if self.car_marker is not None:
            self.mapview.remove_marker(self.car_marker)
        self.car_marker = MapMarker(source='images/car.png', lat=point[0], lon=point[1])
        self.mapview.add_marker(self.car_marker)

    def set_pothole_marker(self, point):
        """
        Встановлює маркер для ями
        :param point: GPS координати
        """
        if self.pothole_marker is not None:
            self.mapview.remove_marker(self.pothole_marker)
        self.pothole_marker = MapMarker(source='images/pothole.png', lat=point[0], lon=point[1])
        self.mapview.add_marker(self.pothole_marker)

    def set_bump_marker(self, point):
        """
        Встановлює маркер для лежачого поліцейського
        :param point: GPS координати
        """
        if self.bump_marker is not None:
            self.mapview.remove_marker(self.bump_marker)
        self.bump_marker = MapMarker(source='images/bump.png', lat=point[0], lon=point[1])
        self.mapview.add_marker(self.bump_marker)

    def add_to_car_route(self, point):
        """
        Додає точку до маршруту авто
        :param point: GPS координати
        """
        self.map_layer.add_point(point)  # Оновлює маршрут на мапі

    def build(self):
        """
        Ініціалізує мапу MapView(zoom, lat, lon)
        :return: мапу
        """
        self.mapview = MapView()
        return self.mapview


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MapViewApp().async_run(async_lib="asyncio"))
    loop.close()
