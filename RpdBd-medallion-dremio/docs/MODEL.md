# Model opoznien: wyniki i ograniczenia

Klasyfikator GBT (Spark MLlib) przewiduje, czy odlot opozni sie o ponad 15 min (`DEPARTURE_DELAY > 15`).
Dane: DOT 2015 (5 lotnisk, 1,27 mln lotow) + godzinowa pogoda Open-Meteo. Podzial czasowy: trening styczen-wrzesien, test pazdziernik-grudzien.
Cechy (`contracts/features.json`): lotnisko, godzina, dzien tygodnia, miesiac, temperatura, wiatr, porywy, opady, zachmurzenie.

| Miara | Wartosc |
| --- | --- |
| AUC (test, klaster, pelne dane) | 0,629 |
| F1 klasy "opozniony" przy progu 0,5 | 0,27 |
| F1 przy progu 0,38 (strojenie progu) | 0,32 |
| AUC tylko z kalendarza (bez pogody) | 0,569 |

Wnioski:
- Pogoda realnie pomaga (+0,06 AUC wzgledem samego kalendarza), ale wiekszosc opoznien wynika z innych przyczyn.
- Cel 0,70 AUC jest nieosiagalny przy tych cechach: strumien nie zna przewoznika, rotacji samolotu ani stanu ruchu. Realny sufit to ok. 0,63-0,66.
- Plytki GBT (glebokosc 3, 50 drzew) jest o ok. 0,01-0,02 AUC lepszy niz glebokosc 5; glebokosc 8 przeucza sie.
- Dlatego `delay_prob` w alertach traktujemy jako slaby sygnal pomocniczy obok prostego wskaznika pogodowego `risk`.
- Model jest z 2015 roku i ocenia "typowy odlot z lotniska", nie konkretny rejs.
