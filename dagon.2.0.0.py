"""Script che sincronizza l'inventario di Discogs con L'inventario locale"""

# librerie di sistema

import os
import glob
from datetime import datetime
from os import path
import sys

import pandas
import yaml

from Discogs_helper.discogsinterface2 import DiscogsInterface2
from Discogs_helper.DiscogsClasses import *
from Logger import logger
import click


# from pathlib import Path


class Dagon:
    def __init__(self):
        config = yaml.safe_load(open("config.yaml"))
        self.inventory_root = config["inventory_root"] + str(datetime.now().strftime("%Y"))
        self.current_folder = os.path.join(self.inventory_root, str(datetime.now().strftime("%m")))
        self.discogs = DiscogsInterface2()
        self.inventory_list = []
        self.lbtuple_list = []
        self._setup_folders()

    def _setup_folders(self):
        """Costruisce l'ecosistema delle cartelle per i salvataggi degli inventari e dei file di sincronizzazione"""
        if not os.path.isdir(self.inventory_root):
            try:
                os.mkdir(self.inventory_root)
            except OSError:
                logger.error("({})$: Impossibile creare la cartella {}.".format(self.__class__.__name__,
                                                                                self.inventory_root))
            else:
                logger.info("({})$: La cartella '{}' è stata creata correttamente.".format(self.__class__.__name__,
                                                                                           self.inventory_root))
            """Crea le directory per ogni mese"""
            root = self.inventory_root
            branches = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
            leaves = ['save', 'syncro']
            for branch in branches:
                for leaf in leaves:
                    try:
                        f = os.path.join(root, branch, leaf)
                        os.makedirs(f)
                    except OSError:
                        logger.error("({})$: Impossibile creare la cartella {}.".format(self.__class__.__name__,
                                                                                        f))
                    else:
                        logger.info("({})$: La cartella '{}' è stata creata correttamente.".format(self.__class__.__name__,
                                                                                                 f))

    def get_inventory(self):
        logger.debug("({})$: Recupero l'inventario".format(self.__class__.__name__))

        if not path.isfile(os.path.join(self.current_folder, "save", self.discogs.inv_file)):
            logger.debug("({})$: Salvo i dati in{}".format(self.__class__.__name__, self.discogs.inv_file))
            response = self.discogs.guarded_get_request(self.discogs.inventory_base_url)

            if response is not None:
                logger.debug("status code: {}".format(response.status_code))
                tot_pages = int(response.json()['pagination']['pages'])
                logger.debug("Total pages: {}".format(tot_pages))
                start_page = int(response.json()['pagination']['page'])
                logger.debug("Start Page:{}".format(start_page))
                self.get_inventory_data(response.json()["listings"])
                self.save_inventory_data()

                for i in range(start_page + 1, tot_pages + 1):
                    next_page = self.discogs.inventory_base_url + "&page=" + str(i)
                    # print("next page: {}".format(next_page))
                    response = self.discogs.guarded_get_request(next_page)
                    if response is not None:
                        current_page = int(response.json()['pagination']['page'])
                        logger.debug("Current Page: {}".format(current_page))
                        self.get_inventory_data(response.json()['listings'])
                        self.save_inventory_data()
                    else:
                        logger.error("({})$:Response is None".format(self.__class__.__name__))
            else:
                logger.error("({})$:Response is None".format(self.__class__.__name__))

    def get_inventory_data(self, listings):
        """Estrae i dati dalla risposta json fornita da Discogs
            Dati che estraggo
            id
            status
            condition
            sleeve_condition
            original_price -> value
            release -> images -> [0] -> uri
                    -> artist
                    -> format
                    -> resource_url
                    -> title"""
        for listing in listings:
            data = ListingData()
            data.id = listing['id']
            data.r_id = listing['release']['id']
            data.format = listing['status']
            data.condition = listing['condition']
            data.sleeve_condition = listing['sleeve_condition']
            data.price = listing['original_price']['value']
            try:
                data.img = listing['release']['images'][0]['uri']
            except IndexError as err:
                logger.warning("({})$: Immagine non trovata per la release {}".format(self.__class__.__name__,
                                                                                      listing['release']['id']))
                logger.warning("({})$: {}".format(self.__class__.__name__, err))
                data.img = None
            data.artist = listing['release']['artist']
            data.format = listing['release']['format']
            data.resource_url = listing['release']['resource_url']
            data.title = listing['release']['title']

            # print("ID:", data.id)
            self.inventory_list.append(data)

    def load_inventory_data(self):
        if not self.inventory_list:
            file_path = os.path.join(self.current_folder, "save", self.discogs.inv_file)
            df = pandas.read_csv(file_path)
            for i, r in df.iterrows():
                d = ListingData()
                d.id = r['id']
                d.r_id = r['release_id']
                d.status = r['status']
                d.condition = r['condition']
                d.sleeve_condition = r['sleeve_condition']
                d.price = r['price']
                d.img = r['img']
                d.artist = r['artist']
                d.format = r['format']
                d.resource_url = r['resource_url']
                d.title = r['title']

                self.inventory_list.append(d)

    def get_barcodes(self):

        self.load_inventory_data()

        i = 0
        while i < len(self.inventory_list):

            logger.debug("resource url: {}".format(self.inventory_list[i].resource_url))
            response = self.discogs.guarded_get_request(self.inventory_list[i].resource_url)

            if response.status_code == 200:

                logger.debug("Response code: {}".format(response.status_code))
                logger.debug('Remaining rate limit: {}'.format(response.headers['X-Discogs-Ratelimit-Remaining']))

                if response.json()['identifiers'] is not None:
                    for j in range(0, len(response.json()['identifiers'])):
                        if response.json()['identifiers'][j]['type'] == "Barcode":
                            lbtuple = ListingBarcodeTuple(lid=self.inventory_list[i].id,
                                                          rid=self.inventory_list[i].r_id,
                                                          bcode=response.json()['identifiers'][j]['value'])
                            self.lbtuple_list.append(lbtuple)
                    self.save_lb_tuple()

                else:
                    # TODO salva in un file i dati della release che non ha identificatori
                    logger.warning(r"La release {id} non ha identificatori. Info: Artista: {artist} - Titolo: {title}".
                                   format(id=self.inventory_list[0].id,
                                          artist=self.inventory_list[0].artist,
                                          title=self.inventory_list[0].title))
                i = i + 1
            elif response.status_code == 429:
                logger.error("({})#: Response code: {}".format(self.__class__.__name__, response.status_code))
                logger.error("({})#: richiesta all'url: {} fallita".format(self.__class__.__name__, self.inventory_list[i].resource_url))
                logger.error("Limite raggiunto. Messaggio:{}".format(response.json()['message']))
                logger.error("Aspetto 120 sec")
                self.discogs.sleep_now()
                # sys.exit(1)
            elif response.status_code == 500:
                logger.error("({})#: Response code: {}".format(self.__class__.__name__, response.status_code))
                logger.error("({})#: richiesta all'url: {} fallita".format(self.__class__.__name__, self.inventory_list[i].resource_url))
                logger.error("Errore del server. Messaggio:{}".format(response.json()['message']))
                logger.error("Aspetto 120 sec")
                self.discogs.sleep_now()
            else:
                logger.error("({})#: Response code: {}".format(self.__class__.__name__, response.status_code))
                logger.error("Aspetto 120 sec")
                self.discogs.sleep_now()



    # SAVING METHODS
    """Metodi per salvare i dati"""

    def save_inventory_data(self):
        """Salva in un file csv i dati dell'inventario"""
        # print(self.current_folder)
        df = pandas.DataFrame().from_records([item.to_dict() for item in self.inventory_list])
        file_path = os.path.join(self.current_folder, "save", self.discogs.inv_file)
        df.to_csv(str(file_path), index=False, header=True, encoding='utf-8-sig')

    def save_lb_tuple(self):
        """Salva in un file csv le coppie listing_id/barcode"""
        df = pandas.DataFrame().from_records([item.to_dict() for item in self.lbtuple_list])
        file_path = os.path.join(self.current_folder, "syncro", self.discogs.lbtuple_file)
        df.to_csv(file_path, index=False, header=True, encoding='utf-8-sig')


@click.command()
@click.option("--i", "-I", "tipo", flag_value="incremental_root",
              help="Recupera i dati dell'inventario Discogs in modo "
                   "incrementale")
@click.option("--n", "-N", "tipo", flag_value="new",
              help="Recupera tutti i dati dell'inventario di Discogs ignorando "
                   "file precendenti")
@click.option("--u", "-U", "tipo", flag_value="update", help="Rimuove le offerte indicate nel file discogs.csv")
@click.option("--c", "-C", "tipo", flag_value="check", help="Controlla il file di sincronizzazione in cerca di "
                                                            "barcode mancanti")
def main(tipo):
    global dagon
    try:

        dagon = Dagon()
        logger.info("({})$: 'I rise from the deep and bringing maddening knowledge'".format(dagon.__class__.__name__))

        if tipo == 'new':
            logger.info("({})$: Sincronizzazione totale dell'inventario".format(dagon.__class__.__name__))
            logger.info("({})$: Scaricamento dell'inventario".format(dagon.__class__.__name__))
            dagon.get_inventory()
            logger.debug("({} Inventario salvato)$:".format(dagon.__class__.__name__))
            dagon.get_barcodes()
            logger.info("({})$: Sincronizzazione totale terminato".format(dagon.__class__.__name__))

        elif tipo == 'incremental_root':
            # TODO creare una routine per aggiornamenti incrementali
            logger.info("({})$: Sincronizzazione inventario incrementale.".format(dagon.__class__.__name__))
            logger.info("({})$: Scaricamento dell'inventario".format(dagon.__class__.__name__))
            dagon.get_inventory()
            logger.debug("({} Inventario salvato)$:".format(dagon.__class__.__name__))
            # p = Path(dagon.generate_saving_path(dagon.discogs.inv_file))
            # p.touch()
        elif tipo == 'update':
            logger.info("({})$: Rimuovo le offerte da Discogs".format(dagon.__class__.__name__))
            fname = "discogs-update.csv"
            data = pandas.read_csv(fname, encoding="utf-8")

            for i, row in data.iterrows():
                print(row['listing_id'])
                url = dagon.discogs.listing_base_url.format(row['listing_id'])
                logger.info("({})$: Rimuovo l'offerta: {}".format(dagon.__class__.__name__, url))
                dagon.discogs.delete_request(url)
        elif tipo == 'check':
            logger.info("({})$: Controllo il file di sincronizzazone".format(dagon.__class__.__name__))
            folder = os.path.join(dagon.current_folder, "syncro", "*")
            list_of_files = glob.glob(folder)  # * means all if need specific format then *.csv
            latest_file = max(list_of_files, key=os.path.getctime)
            df = pandas.read_csv(latest_file)

            nobarcode = df.loc[df["barcode"] == "-1"]
            nobarcode.drop_duplicates(subset=['listing_id'])
            logger.info("({})$: Trovati {} elementi senza barcode.".format(dagon.__class__.__name__,
                                                                           nobarcode.shape[0]))

            nobarcode.to_csv("nobarcode.csv", index=False, header=True, encoding='utf-8-sig')
            logger.info("({})$: Salvo gli elementi senza barcode".format(dagon.__class__.__name__))

        else:

            logger.warning("({})$: Tipo di aggiornamento non selezionato.".format(dagon.__class__.__name__))

        logger.info("({})$: 'I go back to the unfathomable depth where in death"
                    " I shall sleep to rise again when the stars align.'".format(dagon.__class__.__name__))

    except AttributeError as e:
        logger.error("({})$:AttributeError: ".format(dagon.__class__.__name__))
        logger.error(e)
        # print(df.empty)
        sys.exit(1)
    except KeyError as e:
        logger.error("({})$: KeyError: ".format(dagon.__class__.__name__))
        logger.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        now = datetime.now()
        logger.info("({})$: KeyboardInterrupt: Chiusura forzata ".format(dagon.__class__.__name__))

        sys.exit(1)


if __name__ == '__main__':
    main()
