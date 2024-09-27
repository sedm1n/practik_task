import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class PriceMachine:
    def __init__(self, headers: dict, column_names: dict):
        """
        Инициализация класса PriceMachine. Создает основной DataFrame и сохраняет
        маппинг заголовков столбцов.

        :param headers: Словарь для переименования столбцов
        """
        self.headers = headers
        self.column_names = column_names
        self.agregate_price = self.__create_base_df()

    def __get_files(self, folder_path: str = "prices"):
        """
        Возвращает список CSV-файлов в указанной папке, содержащих 'price' в названии.

        :param folder_path: Путь к директории (по умолчанию: prices)
        :return: Список CSV-файлов
        """
        path = os.path.abspath(folder_path)

        csv_files = [
            os.path.join(path, file)
            for file in os.listdir(path)
            if file.endswith(".csv") and "price" in file.lower()
        ]

        return csv_files

    def __read_csv(self, file_path: str):
        """
        Читает CSV-файл и возвращает его содержимое в виде DataFrame.

        :param file_path: Путь к CSV-файлу
        :return: DataFrame с содержимым файла или пустой DataFrame в случае ошибки
        """
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
            return df
        except FileNotFoundError:
            logger.error("Файл не найден: %s ", file_path)
        except pd.errors.EmptyDataError:
            logger.error("Файл пустой: %s ", file_path)
        except Exception as e:
            logger.error("Ошибка при чтении файла  %s : %s ", file_path, str(e))
        return pd.DataFrame()

    def __create_base_df(self):
        """
        Создает основной DataFrame с необходимыми столбцами.

        :return: Пустой DataFrame с предустановленными столбцами
        """
        return pd.DataFrame(columns=list(self.column_names.values()))

    def _add_data_to_base(self, df: pd.DataFrame, filename: str):
        """
        Добавляет данные из DataFrame в основной DataFrame с учетом маппинга столбцов.

        :param df: DataFrame с данными для добавления
        :param filename: Имя файла, из которого были загружены данные
        """
        try:
            temp_df = pd.DataFrame()

            for col in df.columns:
                if col in self.headers:
                    temp_df[self.headers[col]] = df[col]

            temp_df[self.column_names["file"]] = filename
            self.agregate_price = pd.concat(
                [self.agregate_price, temp_df], ignore_index=True
            )
        except KeyError as e:
            logger.error("Отсутствует ожидаемый столбец: %s", str(e))
        except Exception as e:
            logger.error("Ошибка при добавлении данных: %s", str(e))

    def _calucate_price_weight(self):
        """
        Рассчитывает стоимость за килограмм для каждой позиции.
        """
        try:
            self.agregate_price[self.column_names["price_per_kg"]] = (
                self.agregate_price[self.column_names["price"]]
                / self.agregate_price[self.column_names["weight"]]
            ).round(2)
        except ZeroDivisionError:
            logger.error("Вес позиции равен нулю.")
        except TypeError as e:
            logger.error("Значение в позиции не является числом.%s", str(e))

    def load_prices(self, folder_path: str = "prices"):
        """
        Загружает файлы с ценами из указанной директории и добавляет их в основной DataFrame.

        :param folder_path: Путь к папке с файлами (по умолчанию: prices)
        """
        file_list: list[str] = self.__get_files(folder_path)

        for file_path in file_list:
            df = self.__read_csv(file_path)
            self._add_data_to_base(df, os.path.basename(file_path))

    def prepare_data(self):
        """
        Подготавливает итоговый DataFrame .
        Расчитывает цену за килограмм для каждой позиции.
        Сортирует итоговый DataFrame.
        Добавляет столбец "№" с порядковым номером позиции.
        """

        self._calucate_price_weight()
        self.sort_data()
        self.agregate_price.index = range(1, len(self.agregate_price) + 1)
        self.agregate_price.index.name = "№"

    def export_to_html(self, fname="output.html"):
        """
        Экспортирует данные в HTML-файл.

        :param fname: Имя выходного HTML-файла (по умолчанию: output.html)
        """
        html_header = """
            <!DOCTYPE html>
            <html>
            <head>
                  <title>Позиции продуктов</title>
            </head>
            <body>
            """
        html_footer = """
                  </table>
                  </body>
                  </html>
                  """

        try:
            html_table = self.agregate_price.to_html(
                index=False, justify="left", border=0
            )

            full_html = html_header + html_table + html_footer

            with open(fname, "w", encoding="utf-8") as f:
                f.write(full_html)

            print(f"Данные успешно экспортированы в файл {fname}")
        except IOError as e:
            logger.error("Ошибка записи в файл %s: %s", fname, str(e))
        except Exception as e:
            logger.error("Ошибка при экспорте данных: %s", str(e))

    def sort_data(self, field: str = "price_per_kg", order_asc: bool = True):
        """
        Сортирует данные в основном DataFrame по указанному полю.

        :param field: Поле для сортировки (по умолчанию: "цена за кг.")
        :param order_asc: Порядок сортировки, если True — по возрастанию (по умолчанию: True)
        """
        self.agregate_price = self.agregate_price.sort_values(
            by=self.column_names[field], ascending=order_asc
        )

    def find_text(self, text):
        """
        Ищет текст в столбце "название" и возвращает DataFrame с результатами.

        :param text: Строка для поиска
        :return: DataFrame с найденными результатами или пустой DataFrame, если результаты не найдены
        """
        try:
            result_df = self.agregate_price.loc[
                self.agregate_price[self.column_names["name"]].str.contains(
                    text, case=False
                )
            ]
            return result_df
        except KeyError:
            logger.error("Столбец 'название' не найден в данных.")
        except Exception as e:
            logger.error("Ошибка при поиске: %s", str(e))
        return pd.DataFrame()


def print_menu():
    """
    Отображает меню действий и возвращает выбор пользователя.

    :return: Строка, выбранная пользователем
    """
    menu = """
    Меню:
    1. Вывести данные
    2. Экспортировать в HTML
    3. Поиск

    exit. Выход
    """
    print(menu)
    return input("Выберите действие: ")


def handle_show_data(pm):
    """
    Обрабатывает команду показа данных.

    :param pm: Экземпляр класса PriceMachine
    """
    print(pm.agregate_price.to_string(index=True, index_names=True, justify="left"))


def handle_export_html(pm):
    """
    Обрабатывает команду экспорта данных в HTML.

    :param pm: Экземпляр класса PriceMachine
    """
    pm.export_to_html()


def handle_search(pm):
    """
    Обрабатывает команду поиска данных по тексту.

    :param pm: Экземпляр класса PriceMachine
    """
    while True:
        print("\nВведите текст для поиска (или 'exit' для выхода): ")
        search_text = input()
        if search_text == "exit":
            break

        finded_text = pm.find_text(search_text)
        if finded_text.empty:
            print("Элемент не найден")
        else:
            print(f"Всего найдено: {finded_text.shape[0]} шт.")
            print(finded_text.to_string(index=True, index_names=True, justify="left"))


def main():
    """
    Основная функция программы, управляющая работой меню и взаимодействием с пользователем.
    """
    headers = {
        "название": "название",
        "продукт": "название",
        "товар": "название",
        "наименование": "название",
        "цена": "цена",
        "розница": "цена",
        "фасовка": "вес",
        "масса": "вес",
        "вес": "вес",
    }

    colunm_names = {
        "name": "название",
        "price": "цена",
        "weight": "вес",
        "file": "файл",
        "price_per_kg": "цена за кг.",
    }

    pm = PriceMachine(headers=headers, column_names=colunm_names)
    pm.load_prices()
    pm.prepare_data()

    pd.set_option("display.colheader_justify", "left")
    pd.set_option("display.float_format", "{:.2f}".format)

    commands = {
        "1": handle_show_data,
        "2": handle_export_html,
        "3": handle_search,
    }

    while True:
        user_choice = print_menu()

        if user_choice == "exit":
            print("Завершение программы.")
            break

        command = commands.get(user_choice)
        if command:
            command(pm)
        else:
            print("Неверный выбор, попробуйте снова.")


if __name__ == "__main__":
    main()
