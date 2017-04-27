#!/usr/bin/python
# coding=utf-8

import urllib.request
from statistics import StatisticsError, mean
from bs4 import BeautifulSoup
from collections import namedtuple, OrderedDict
from operator import attrgetter
from multiprocessing.dummy import Pool as ThreadPool

# Collection of Thread IDs in several categories
Produktthreads = OrderedDict([
    ('Spielhilfen', [
100244	, 
99039	, 
98315	, 
100685	, 
100991	, 
97748	, 
97677	, 
99127	, 
99440	, 
101317	, 
100963	, 
100256	, 
98177	, 
100357	, 
100687	, 
98795	, 
98410	, 
97871	, 
97778	, 
98203	, 
100990	, 
101730	, 
98041	, 
99475	, 
97678	, 
96824	, 
98680	, 
99578	, 
97750	, 
97572	, 
98042	, 
101318	, 
97791	, 
97395	, 
97528	, 
99447	, 
98850	, 
97897	, 
97942	, 
100964	, 
97870	, 
97099	, 
98694	, 
97185	, 
98844	, 
97777	, 
97484	, 
97574	, 
97986	, 
98288	, 
98597	, 
99126	, 
99476	, 
99764	, 
100524	, 
101286	, 
96865	, 
97282	, 
97790	, 
97037	, 
99528	, 
98067	, 
96941	, 
98756	, 
99269	, 
99119	, 
98956	, 
97985	, 
97809	, 
97896	, 
100989	, 
101972	, 
100359	, 
97396	, 
97943	, 
98066	, 
99208	, 
97486	, 
97283	, 
98794	, 
99337	, 
100995	, 
97097	, 
100358	, 
99848	, 
98125	, 
97485	, 
96864	, 
96940	, 
100522	, 
101970	, 
96806	, 
98124	, 
98065	, 
99191	, 
99174	, 
101260	, 
98287	, 
98289	, 
97573	, 
99117	, 
99577	, 
100245	, 
100627	, 
101971	, 
98679	, 
99040	, 
99192	, 
97810	, 
100683	, 
98316	, 
99259	, 
99336	, 
100965	, 
97397	, 
98205	, 
100682	, 
98228	, 
99207	, 
100251	, 
101415	, 
96805	, 
98227	, 
100856	, 
98175	, 
99511	, 
101316	, 
97039	, 
101259	, 
99446	, 
98717	, 
99270	, 
98176	, 
98714	, 
98849	,
102106,
102107,
102183,
102184,
102108,
102142,
102126,
]),
    ('Abenteuer', [
97188	, 
98757	, 
100657	, 
98141	, 
98381	, 
95786	, 
97487	, 
69636	, 
100252	, 
97488	, 
101504	, 
97286	, 
95709	, 
95620	, 
99306	, 
98418	, 
99281	, 
99247	, 
100523	, 
100612	, 
97840	, 
95961	, 
97767	, 
96509	, 
97187	, 
97186	, 
97675	, 
18975	, 
98266	, 
98267	, 
99055	, 
99260	, 
96344	, 
96343	, 
96270	, 
99616	, 
95711	, 
99357	, 
96508	, 
95788	, 
98843	, 
95785	, 
98013	, 
98647	, 
95552	, 
97399	, 
97971	, 
98053	, 
97400	, 
97911	, 
96823	, 
97287	, 
97576	, 
97828	, 
97841	, 
98054	, 
99054	, 
99193	, 
98088	, 
99765	, 
99541	, 
100656	, 
101450	, 
101451	, 
101503	, 
95556	, 
98308	, 
98089	, 
96939	, 
96510	, 
96422	, 
100628	, 
96345	, 
96423	, 
99346	, 
95899	, 
98646	, 
98954	, 
97038	, 
98977	, 
95621	, 
98687	, 
95622	, 
97098	, 
98976	, 
95559	, 
95898	, 
97285	, 
97674	, 
98309	, 
98382	, 
99056	, 
99307	, 
97676	, 
98140	, 
96507	, 
99847	, 
95710	, 
95900	, 
96825	, 
97575	, 
97827	, 
99529	, 
101261	, 
96098	, 
97912	, 
96099	, 
95553	, 
96421	, 
95619	, 
96654	, 
97284	, 
98975	, 
98974	, 
96189	, 
97829	, 
98012	, 
98759	, 
99173	, 
99345	, 
99248	, 
99282	, 
101262	, 
101416	, 
97969	, 
96689	, 
96342	, 
95618	, 
96690	, 
95560	, 
99118	, 
98265	, 
100857	, 
96420	, 
96269	, 
96028	, 
96027	, 
96026	, 
98216	, 
96012	, 
99194	, 
99542	, 
98758	, 
99051	, 
99510	, 
101501	, 
101502	, 
101728	,
102161,
])
])

# maintain anthologies separately
Anthologien = OrderedDict([
                           ('Cthulhu - Ars Mathematica',[
                                            102159, 102160, 102158]),
                           ('Cthulhu - Dreissig',[
                                            101501, 101503, 101504, 101502]),
                           ('Cthulhu - The Final Revelation',[
                                            97284, 97285, 97286, 97287]),
                           ('Cthulhu - Die Goldenen Hände Suc´naaths',[
                                            98758, 98757, 98759]),
                           ('Shadowrun - Licht aus der Asche',[
                                            96028, 96027, 96026])
                           ])

# Add anthologies to collection to avoid duplicates
for Anthologie in Anthologien:
    for threadid in Anthologien[Anthologie]:
        if threadid not in Produktthreads['Abenteuer']:
            Produktthreads['Abenteuer'].append(threadid)

# URL of a thread (%d will be thread_id)
baseurl = "https://www.tanelorn.net/index.php?topic=%d.0"

# Number of parallel threads (should be equal to number of CPU cores)
concurrent_parses = 4


def bbcode(tag, string, value=None):
    """Return a text(string) enclosed by the bbcode tags"""
    if value:
        return'[' + tag + '=' + value + ']' + string + '[/' + tag + ']'
    else:
        return'[' + tag + ']' + string + '[/' + tag + ']'


def bbcodeurl(urlstring, urlname):
    """Return an bbcode url format for given url and description"""
    return bbcode('url', urlname, urlstring)


def bbbold(text):
    """Return the text with a bbcode bold tag"""
    return bbcode(tag='b', string=text)


def bbtt(text):
    """Return the text with a bbcode tt tag"""
    return bbcode(tag='tt', string=text)


class bbtable():

    """creates the frame of a bbcode table"""

    def __init__(self, rows):
        """needs the rows as input for this table"""
        self.elements = rows

    def tablify(self, rows):
        """adds start and end tags for tables"""
        return str('[table]\r\n' + rows + '[/table]')

    def __str__(self):
        """prints table in bbcode format"""
        return(self.tablify(''.join(str(row) for row in self.elements)))


class tablerow(bbtable):

    """creates a bbcode table row with correct tags"""

    def cellify(self, rowfield):
        """encloses cells with correct tags"""
        return str('[td]' + str(rowfield) + '[/td]')

    def rowify(self, cells):
        """encloses rows with the correct tags"""
        return str('[tr]' + str(cells) + '[/tr]\r\n')

    def __str__(self):
        """adds cell and row tags to elements"""
        return(self.rowify(''.join(self.cellify(field) for field in self.elements)))


class tableheaderrow(tablerow):

    """adds a header row"""

    def cellify(self, rowfield):
        return str('[td]' + bbbold(rowfield) + bbtt('   ') + '[/td]')


class ProduktParser():

    def __init__(self, Produktthreads, Produkt = namedtuple('Produkt', 'name id url Stimmen Durchschnitt'), Produkte = [], Anthologien = [], baseurl = baseurl):
        """set base properties: URLs, thread ids, format"""
        self.Produkt = Produkt
        self.Produkte = Produkte
        self.baseurl = baseurl
        self.Produktthreads = Produktthreads
        self.Anthologien = Anthologien
        self.bewertungen = set(
            [item for sublist in self.Produktthreads.values() for item in sublist])
        self.pool = ThreadPool(concurrent_parses)
        self.pool.map(self.getProdukt, self.bewertungen)
        self.getAnthologie()

    def getProdukt(self, threadid):
        """collect information for selected thread id"""
        url = self.baseurl % threadid
        page = urllib.request.urlopen(url)
        soup = BeautifulSoup(page.read(), "html.parser")
        Produktname = soup.find('title').string.split('/')[0].strip()
        polls = soup.find('dl', {'class': 'options'})
        options = polls.findAll('dt', {'class': 'middletext'})
        votes = polls.findAll('span', {'class': 'percentage'})        
        ergebnis = dict(zip([[int(s) for s in option.string.replace("(","").split() if s.isdigit()][0] for option in options], [int(vote.string.split(' ')[0]) for vote in votes]))
        einzelvotes = [
            item for sublist in [[k] * v for k, v in ergebnis.items()] for item in sublist]
        try:
            durchschnitt = str(round(mean(einzelvotes), 2))
            stimmen = len(einzelvotes)
        except (ZeroDivisionError, StatisticsError) as e:
            durchschnitt = '0 / No votes yet'
            stimmen = 0
        self.Produkte.append(
            self.Produkt(Produktname, threadid, url, stimmen, durchschnitt))
        
    def getAnthologie(self):
        for Anthologie in self.Anthologien:
            Anthologiedurchschnittagg = 0
            Anthologiestimmen = 0
            for Spielhilfe in self.Produkte:
                if Spielhilfe.id in self.Anthologien[Anthologie]:
                    if  Spielhilfe.Durchschnitt != '0 / No votes yet':
                        Anthologiestimmen += Spielhilfe.Stimmen  
                        Anthologiedurchschnittagg += Spielhilfe.Stimmen * float(Spielhilfe.Durchschnitt)
            if Anthologiestimmen == 0:
                Anthologiedurchschnitt = '0 / No votes yet'
            else:
                Anthologiedurchschnitt = str(round(Anthologiedurchschnittagg/Anthologiestimmen, 2))
                          
            self.Produkte.append(
                self.Produkt(Anthologie, 0, 0, Anthologiestimmen, Anthologiedurchschnitt))
                    

    def generateTable(self, bewertungsthreads):
        """"generate a table for the threads"""
        return bbtable([tableheaderrow(['Platz', 'Bewertung', 'Stimmen', 'Produkt'])]
                       + [tablerow([index + 1, element.Durchschnitt, element.Stimmen, bbcodeurl(element.url, element.name)])
                          for index, element in enumerate(sorted(bewertungsthreads, key=attrgetter('Durchschnitt'), reverse=True))])

    def printProdukte(self):
        """"print the table"""
        for key, value in self.Produktthreads.items():
            print('\r\n' + bbbold(key))
            print(self.generateTable(
                [Spielhilfe for Spielhilfe in self.Produkte if Spielhilfe.id in value]))
            
        print('\r\n' + bbbold("Anthologien"))
        print(self.generateTable(
            [Spielhilfe for Spielhilfe in self.Produkte if Spielhilfe.name in [Anthologie for Anthologie in Anthologien]]))


if __name__ == '__main__':
    TanelornParser = ProduktParser(Produktthreads=Produktthreads, Anthologien=Anthologien)
    print(
        'Hier die Sammlung aller Produktbewertungsthreads, inklusive Durchschnittsbewertung und Ranking.')
    print(
        'Das script ist verfügbar unter https://github.com/zaboron')
    TanelornParser.printProdukte()
