from enum import Enum
from typing import List

class Country(Enum):
    """Enum class representing a country"""
    AT = "AT"
    BE = "BE"
    BG = "BG"
    CH = "CH"
    CY = "CY"
    CZ = "CZ"
    DK = "DK"
    EE = "EE"
    EL = "EL"
    ES = "ES"
    EU27_2020 = "EU27_2020"
    FI = "FI"
    FR = "FR"
    HR = "HR"
    HU = "HU"
    IS = "IS"
    IT = "IT"
    LI = "LI"
    LT = "LT"
    LU = "LU"
    LV = "LV"
    MT = "MT"
    NL = "NL"
    NO = "NO"
    PL = "PL"
    PT = "PT"
    RO = "RO"
    SE = "SE"
    SI = "SI"
    SK = "SK"
    DE = "DE"
    DE_TOT = "DE_TOT"
    AL = "AL"
    EA18 = "EA18"
    EA19 = "EA19"
    EFTA = "EFTA"
    IE = "IE"
    ME = "ME"
    MK = "MK"
    RS = "RS"
    AM = "AM"
    AZ = "AZ"
    GE = "GE"
    TR = "TR"
    UA = "UA"
    BYAAAA = "BY"
    EEA30_2007 = "EEA30_2007"
    EEA3 = "EEA3"

    @classmethod
    def get_countries(cls) -> List[str]:
        """Get a list with all the countries"""
        all_countries = set()
        for _, possible_country in cls.__members__.items():
            if len(possible_country.value) == 2:
                all_countries.add(possible_country.value)
        return sorted(list(all_countries))
