#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

/* wątki */
#include <pthread.h>

/* stałe dot zasobów */
#define SEA_SLOTS 20 // maksymalna liczba turystów na morzu
#define VEHICLE_SLOTS 10 // liczba pojazdów
#define TECHNICIAN_SLOTS 4 // liczba techników
#define VEHICLE_STARTING_HEALTH 10 // maksymalna wartość stanu technicznego pojazdu
#define VEHICLE_HEALTH_MAX_DECLINE 3 // maksymalna wartość ubytku stanu technicznego pojazdu po jednej wycieczce

/* boolean */
#define TRUE 1
#define FALSE 0

/* typy wiadomości */
#define MSG_TYPE_REQ 1
#define MSG_TYPE_REL 2
#define MSG_TYPE_OK 3

/* możliwe stany */
#define STATE_AWAIT_TOURISTS 1 // oczekiwanie na osoby
#define STATE_AWAIT_SEA_ACCESS 2 // oczekiwanie na dostęp do morza
#define STATE_AWAIT_VEHICLE_ACCESS 3 // oczekiwanie na pojazd
#define STATE_TOUR_IN_PROGRESS 4 // wycieczka trwa
#define STATE_AWAIT_TECHNICIAN 5 // oczekiwanie na technika
#define STATE_REPAIR_IN_PROGRESS 6 // oczekiwanie na zakończenie naprawy przez technika

/* możliwe typy zasobów */
#define RESOURCE_SEA 1 // morze
#define RESOURCE_VEHICLE 2 // pojazd
#define RESOURCE_TECHNICIAN 3 // technik

/* TYPY WIADOMOŚCI */

// wiadomość REQ
typedef struct {
    int timestamp; // znacznika zeg. Lamporta
    int resource_type; // typ zasobu
    int resource_id; // id zasobu (w przypadku rozróżnialnego - pojazdu)
    // w przypadku dostępu do morza - resource_id oznacza liczbę uczestników
} msg_REQ;

// wiadomość REL
typedef struct {
    int timestamp; // znacznika zeg. Lamporta
    int resource_type; // typ zasobu
    int resource_id; // id zasobu (w przypadku rozróżnialnego - pojazdu)
    int resource_update_value; // zaktualizowana wartość zasobu (zużycie pojazdu)
} msg_REL;

// wiadomość OK
typedef struct {
    int timestamp; // znacznika zeg. Lamporta
} msg_OK;

/* FUNKCJE WYSYŁAJĄCE WIADOMOŚCI */

void sendREQ(int resource_type, int resource_id, int source, int count) {

    msg_REQ msg;
    msg.resource_id = resource_id;
    msg.resource_type = resource_type;
    msg.timestamp = ++l_clock;
    
    for (int i = 0; i < count; i++) {
        MPI_Send(&msg, sizeof(msg), MPI_INT, i, MSG_TYPE_REQ, MPI_COMM_WORLD);
    }
}

void sendREL(int resource_type, int resource_id, int resource_update_value, int source, int count) {

    msg_REL msg;
    msg.resource_id = resource_id;
    msg.resource_type = resource_type;
    msg.resource_update_value = resource_update_value;
    msg.timestamp = ++l_clock;

    for (int i = 0; i < count; i++) {
        MPI_Send(&msg, sizeof(msg), MPI_INT, i, MSG_TYPE_REL, MPI_COMM_WORLD);
    }
}

void sendOK(int target) {

    msg_OK msg;
    msg.timestamp = ++l_clock;

    MPI_Send(&msg, sizeof(msg), MPI_INT, target, MSG_TYPE_OK, MPI_COMM_WORLD);
}

/* REPREZENTACJA STANU PRZEWODNIKA */

// mutex chroniący dostęp do stanu przewodnika
pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

// obecny stan
int current_state = STATE_AWAIT_TOURISTS;

// zegar Lamporta
int l_clock = 0;

// liczba odebranych OK
int ok_count = 0;

// kiedy wysłaliśmy ostatnie REQ
int req_timestamp = 0;

// id pojazdu, który obecnie używamy
int current_vehicle_id = -1;

// kolejka do morza
typedef struct {
    int client_id;
    int count;
    int timestamp;
    seq_queue_el *next;
} sea_queue_el;

sea_queue_el *sea_queue=NULL;

// tablica kolejek do pojazdów
typedef struct {
    int client_id;
    int timestamp;
    vehicle_queue_el *next;
} vehicle_queue_el;

vehicle_queue_el *vehicle_queue[VEHICLE_SLOTS];

// kolejka do techników
typedef struct {
    int client_id;
    int timestamp;
    technician_queue_el *next;
} technician_queue_el;

technician_queue_el *technician_queue = NULL;

// tablica stanów pojazdów
int vehicle_health[VEHICLE_SLOTS];

// lista numerów pojazdów w kolejce do których stoimy
// TODO
// na początek uproszczone - jednocześnie stoimy w kolejce do jednego pojazdu
int awaited_vehicle_id = -1;

int size, rank;

/* TWORZONE WĄTKI */

// wątek oczekujący na turystów
void *awaitTouristsThread(void *ptr)
{
    log(rank, l_clock, "Czekam na turystow...");

    // losowy sleep 1s do 3s
    usleep(rand() % 2000000 + 1000000);

    // wylosuj liczbę użytkowników
    int tourists = rand() % 10 + 1;

    log(rank, l_clock, "Wylosowano liczbe turystow.");

    // mutex on
    pthread_mutex_lock(&mut);

    log(rank, l_clock, "Prosze o dostep do morza.");

    // wyślij REQ do morza
    sendREQ(RESOURCE_SEA, 0, rank, size);

    // zmień stan na 2 (czekamy na dostęp do morza)
    current_state = STATE_AWAIT_SEA_ACCESS;
    ok_count = 0;

    // mutex off
    pthread_mutex_lock(&mut);
}

// wątek oczekujący na zakończenie wycieczki
void *awaitTourEndThread(void *ptr)
{
    // losowy sleep 1s do 3s
    usleep(rand() % 2000000 + 1000000);
    log(rank, l_clock, "Koniec wycieczki.");

    // mutex on
    pthread_mutex_lock(&mut);

    // wylosuj spadek stanu technicznego pojazdu
    int damage = rand() % VEHICLE_HEALTH_MAX_DECLINE + 1;
    int new_health = vehicle_health[current_vehicle_id] - damage;

    // jeśli nowy stan techniczny > 0
    if (new_health > 0) {
        log(rank, l_clock, "Pojazd jeszcze sie nie popsul, zwalniam pojazd i morze.");
        // wyślij REL dla pojazdu
        sendREL(RESOURCE_VEHICLE, current_vehicle_id, new_health, rank, size);
        
        // wyślij REL dla morza
        sendREL(RESOURCE_SEA, 0, 0, rank, size);

        // przejdź do stanu 1 - oczekuj na turystów
        current_state = STATE_AWAIT_TOURISTS;

        // tworzymy wątek czekający na turystów
        pthread_t threadStart;
        pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    }
    else
    {
        log(rank, l_clock, "Pojazd sie popsul, prosze o technika.");
        // wpp - jeśli pojazd się popsuł
        // wyślij REQ dla technika
        ok_count = 0;
        sendREQ(RESOURCE_TECHNICIAN, 0, rank, size);
        // przejdź do stanu 5 - oczekuj na technika
        current_state = STATE_AWAIT_TECHNICIAN;
    }
    // mutex off
    pthread_mutex_unlock(&mut);
}

// wątek oczekujący na zakończenie naprawy
void *awaitVehicleRepairThread(void *ptr)
{
    // losowy sleep 1s do 3s
    usleep(rand() % 2000000 + 1000000);


    log(rank, l_clock, "Naprawa skonczona, zwalniam technika, pojazd i morze.");

    // mutex on
    pthread_mutex_lock(&mut);
    

    // REL dla technika
    sendREL(RESOURCE_TECHNICIAN, 0, 0, rank, size);

    // REL dla pojazdu z MAX stanem technicznym
    sendREL(RESOURCE_VEHICLE, current_vehicle_id, VEHICLE_STARTING_HEALTH, rank, size);

    // REL dla morza
    sendREL(RESOURCE_SEA, 0, 0, rank, size);

    // przejdź do stanu 1 - oczekuj na turystów
    current_state = STATE_AWAIT_TOURISTS;
    
    // tworzymy wątek czekający na turystów
    pthread_t threadStart;
    pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    // mutex off
    pthread_mutex_unlock(&mut);
}

// funkcja logująca (w konsoli)

void log(int id, int timestamp, char *message) {
    printf("[P%02d][t=%03d] %s\n", id, timestamp, message);
}

// funkcje odpowiedzialne za zarządzanie kolejkami

// updateRel, updateReq, findShortestVehicleQueue
// canAccessTechnician, canAccessSea, canAccessVehicle

// zwraca informację czy można uzyskać dostęp do morza
int canAccessSea(int rank)
{
    sea_queue_el *cur = sea_queue;
    int s = 0;
    while (cur != NULL) {
        s += cur->count;
        if (cur->client_id == rank) {
            if (s <= SEA_SLOTS)
                return TRUE;
            else
            {
                return FALSE;
            }
            
        }
        cur = cur->next;
    }
    return FALSE;
}

// zwraca informację T/F czy można uzyskać dostęp do danego pojazdu
int canAccessVehicle(int rank, int vehicle_id) 
{
    if (vehicle_queue[vehicle_id]->client_id != rank) {
        return FALSE;
    } else {
        return TRUE;
    }
}

// --- czy można uzyskać dostęp do technika
int canAccessTechnician(int rank)
{
    technician_queue_el *cur = technician_queue;
    int i = 1;
    while (cur != NULL) {
        if (cur->client_id == rank) {
            if (i <= TECHNICIAN_SLOTS)
                return TRUE;
            else
            {
                return FALSE;
            }
            
        }
        cur = cur->next;
    }
    return FALSE;
}

// zwraca id pojazdu o najkrótszej kolejce
int findShortestVehicleQueue(int rank)
{
    int shortest_len = 2147483647;
    vehicle_queue_el *cur;
    int shortest_id = -1;
    for (int i = 0; i < VEHICLE_SLOTS; i++) {
        cur = vehicle_queue[i];
        int l = 0;
        while (cur != NULL) {
            l++;
            if (cur->client_id == rank) {
                l += 1000;
                break;
            }
            cur = cur->next;
        }
        if (l < shortest_len) {
            shortest_len = l;
            shortest_id = i;
        }
    }
    return shortest_id;
}

// aktualizuje kolejkę w oparciu o REL danego typu
void updateRel(int resource_type, int resource_id, int resource_update_value, int client_id)
{
    switch(resource_type) {
        case RESOURCE_SEA:
            sea_queue_el *prev = NULL;
            sea_queue_el *sq = sea_queue;
            while (sq != NULL) {
                if (sq->client_id == client_id) {
                    if (prev == NULL) {
                        sea_queue = sq->next;
                        delete sq;
                        return;
                    } else {
                        prev->next = sq->next;
                        delete sq;
                        return;
                    }
                }
                prev = sq;
                sq = sq->next;
            }           

            break;
        case RESOURCE_VEHICLE:
            vehicle_queue_el *prev_vehicle = NULL;
            vehicle_queue_el *vq = vehicle_queue[resource_id];
            while(vq != NULL) {
                if (vq->client_id == client_id) {
                    if (prev_vehicle == NULL) {
                        vehicle_queue[resource_id] = vq->next;
                        delete vq;
                        return;
                    } else {
                        prev_vehicle->next = vq->next;
                        delete vq;
                        return;
                    }
                }
                prev_vehicle = vq;
                vq = vq->next;
            }
            if (resource_update_value > 0) {
                vehicle_health[resource_id] = resource_update_value;
            }            
            break;
        case RESOURCE_TECHNICIAN:
            technician_queue_el *prev_tech = NULL;
            technician_queue_el *tq = technician_queue;
            while (tq != NULL) {
                if (tq->client_id == client_id) {
                    if (prev_tech == NULL) {
                        technician_queue = tq->next;
                        delete tq;
                        return;
                    } else {
                        prev_tech->next = tq->next;
                        delete tq;
                        return;
                    }
                }
                prev_tech = tq;
                tq = tq->next;
            }
            break;
    }
}

// aktualizuje kolejkę w oparciu o REQ danego typu - dodaje element do kolejki danego zasobu
void updateReq(int resource_type, int resource_id, int timestamp, int client_id)
{
    switch(resource_type) {
        case RESOURCE_SEA:
            sea_queue_el *n = new sea_queue_el;
            n->client_id = client_id;
            n->timestamp = timestamp;
            n->count = resource_id;

            sea_queue_el *prev = NULL;
            sea_queue_el *sq = sea_queue;
            while (sq != NULL) {
                if (sq->timestamp > timestamp || (sq->timestamp == timestamp && sq->client_id < client_id)) {
                    if (prev == NULL) {
                        n->next = sea_queue;
                        sea_queue = n;
                        return;
                    } else {
                        n->next = sq;
                        prev->next = n;
                        return;
                    }
                }
                prev = sq;
                sq = sq->next;
            }  
            if (sq == NULL) {
                prev->next = n;
                n->next = NULL;
            }         
            break;
        case RESOURCE_VEHICLE:
            vehicle_queue_el *nv = new vehicle_queue_el;
            nv->client_id = client_id;
            nv->timestamp = timestamp;

            vehicle_queue_el *prev_vehicle = NULL;
            vehicle_queue_el *vq = vehicle_queue[resource_id];
            while(vq != NULL) {
                if (vq->timestamp > timestamp || (vq->timestamp == timestamp && vq->client_id < client_id)) {
                    if (prev_vehicle == NULL) {
                        nv->next = vehicle_queue[resource_id];
                        vehicle_queue[resource_id] = nv;
                        return;
                    } else {
                        nv->next = vq;
                        prev_vehicle->next = nv;
                        return;
                    }
                }
                prev_vehicle = vq;
                vq = vq->next;
            }
            if (vq == NULL) {
                prev_vehicle->next = nv;
                nv->next = NULL;
            }           
            break;
        case RESOURCE_TECHNICIAN:
            technician_queue_el *nt = new technician_queue_el;
            nt->timestamp = timestamp;
            nt->client_id = client_id;

            technician_queue_el *prev_tech = NULL;
            technician_queue_el *tq = technician_queue;
            while (tq != NULL) {
                if (tq->timestamp > timestamp || (tq->timestamp == timestamp && tq->client_id < client_id)) {
                    if (prev_tech == NULL) {
                        nt->next = technician_queue;
                        technician_queue = nt;
                        return;
                    } else {
                        nt->next = tq;
                        prev_tech->next = nt;
                        return;
                    }
                }
                prev_tech = tq;
                tq = tq->next;
            }
            if (tq == NULL) {
                prev_tech->next = nv;
                nv->next = NULL;
            }
            break;
    }
}


// główna funkcja
int main(int argc, char **argv)
{
    srand(time(0));

    printf("poczatek\n");
    int provided;

    MPI_Init_thread(&argc, &argv,MPI_THREAD_MULTIPLE, &provided);

    printf("THREAD SUPPORT: %d\n", provided);
    switch (provided) {
        case MPI_THREAD_SINGLE: 
            printf("Brak wsparcia dla wątków, kończę\n");
            /* Nie ma co, trzeba wychodzić */
	    fprintf(stderr, "Brak wystarczającego wsparcia dla wątków - wychodzę!\n");
	    MPI_Finalize();
	    exit(-1);
	    break;
        case MPI_THREAD_FUNNELED: 
            printf("tylko te wątki, ktore wykonaly mpi_init_thread mogą wykonać wołania do biblioteki mpi\n");
	    break;
        case MPI_THREAD_SERIALIZED: 
            /* Potrzebne zamki wokół wywołań biblioteki MPI */
            printf("tylko jeden watek naraz może wykonać wołania do biblioteki MPI\n");
	    break;
        case MPI_THREAD_MULTIPLE: printf("Pełne wsparcie dla wątków\n");
	    break;
        default: printf("Nikt nic nie wie\n");
    }

    // inicjalizacja 

    for (int i = 0; i < VEHICLE_SLOTS; i++) {
        vehicle_health[i] = VEHICLE_STARTING_HEALTH;
    }

    MPI_Status status;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // tworzymy wątek oczekujący na zgłoszenie się turystów
    pthread_t threadStart;
    pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    // główna pętla

    while(true) {

        // czekamy na wiadomości
        msg_REL msg;
        MPI_Recv( &msg, 4, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        int msg_type = status.MPI_TAG;
        int sender = status.MPI_SOURCE;

        // zakładamy mutex
        pthread_mutex_lock(&mut);

        // aktualizacja zeg. Lamporta
        if (msg.timestamp > l_clock) {
            l_clock = msg.timestamp + 1;
        } else {
            l_clock++;
        }
        
        // maszyna stanów
        switch(current_state) {
            case STATE_AWAIT_TOURISTS:
                // czekamy na turystów - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                } 
                if (msgtype == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }     
                break;

            case STATE_AWAIT_SEA_ACCESS:
                // oczekujemy na dostęp do morza
                // tutaj chcemy zliczać liczbę OKejek które dostajemy
                // i jeśli jest ona dostatecznie wysoka - wchodzimy
                if (msg_type == MSG_TYPE_OK) {
                    if (msg.timestamp > req_timestamp) {
                        ok_count++;
                        if (ok_count == size) { //już wszystkie procesy zaktualizowały informacje
                            if (canAccessSea(rank)) {
                                current_state = STATE_AWAIT_VEHICLE_ACCESS;
                                ok_count = 0;
                                int best_vehicle = findShortestVehicleQueue();
                                awaited_vehicle_id = best_vehicle;
                                sendREQ(RESOURCE_VEHICLE, best_vehicle, rank, size);
                                log(rank, l_clock, "Mam juz dostep do morza, prosze o pojazd.");
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                    if (msg.resource_type == RESOURCE_SEA) { //REL dotyczył dostępu do morza
                        if (ok_count == size) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessSea(rank)) {
                                current_state = STATE_AWAIT_VEHICLE_ACCESS;
                                ok_count = 0;
                                int best_vehicle = findShortestVehicleQueue();
                                awaited_vehicle_id = best_vehicle;
                                sendREQ(RESOURCE_VEHICLE, best_vehicle, rank, size);
                                log(rank, l_clock, "Mam juz dostep do morza, prosze o pojazd.");
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }
                break;

            case STATE_AWAIT_VEHICLE_ACCESS:
                // jeśli wiadomość to OK to zwiększamy OK
                // jeśli dostaliśmy OK od wszystkich, to sprawdzamy czy można wejść
                //     jeśli nie można to ustawiamy się w kolejce do jeszcze innego
                //     jeśli można to wchodzimy (wysyłając rel do każdego innego)
                // jeśli wiadomośc to REL to aktualizujemy kolejkę
                //     i jeśli otrzymaliśmy komplet OK to sprawdzamy czy możemy wejść (jeśli rel dotyczy pojazdu w kolejce do którego stoimy)
                // jeśli wiadomość to REQ to aktualizujemy odpowiednią kolejkę
                // spore TODO
                // na początek wersja uproszczona - jednocześnie stoimi w kolejce do tylko jednego pojazdu

                if (msg_type == MSG_TYPE_OK) { // jeśli odbieramy OK
                    if (msg.timestamp > req_timestamp) { // to sprawdzamy czy to OK o które prosiliśmy
                        ok_count++;
                        if (ok_count == size) { // jeśli mamy komplet OK
                            if (canAccessVehicle(rank, awaited_vehicle_id)) { //jeśli możemy wejść do pojazdu to wchodzimy
                                current_state = STATE_TOUR_IN_PROGRESS;
                                ok_count = 0;
                                log(rank, l_clock, "Mam pojazd, rozpoczynam wycieczke.");
                                pthread_t threadTour;
                                pthread_create( &threadTour, NULL, awaitTourEndThread, 0);                                
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) { // jeśli odbieramy rel
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender); // to aktualizujemy kolejkę
                    if (msg.resource_type == RESOURCE_VEHICLE && ok_count == size && msg.resource_id == awaited_vehicle_id) { // i jeśli rel dotyzcył pojazdu w kolejce
                    //  do którego stoimy
                        if (canAccessVehicle(rank, awaited_vehicle_id)) { // to próbujemy do niego wejść (jeśli możemy)
                            current_state = STATE_TOUR_IN_PROGRESS;
                            ok_count = 0;
                            log(rank, l_clock, "Mam pojazd, rozpoczynam wycieczke.");
                            pthread_t threadTour2;
                            pthread_create( &threadTour2, NULL, awaitTourEndThread, 0);  
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) { // jeśli to req, to aktualizujemy kolejkę i odsyłamy OKejkę
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }

                break;

            case STATE_TOUR_IN_PROGRESS:
                // czekamy na koniec wycieczki - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                }  
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                }
                if (msgtype == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }  
                break;
            
            case STATE_AWAIT_TECHNICIAN:
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++;
                    if (ok_count == size) { //już wszystkie procesy zaktualizowały informacje
                        if (canAccessTechnician(rank)) {
                            log(rank, l_clock, "Yay! Dostalem technika.");
                            // jeśli mogę to korzystam z usługi technika
                            current_state = STATE_REPAIR_IN_PROGRESS;
                            ok_count = 0;
                            // tworzymy nowy wątek oczekiwania na koniec naprawy
                            pthread_t threadA;
                            pthread_create( &threadA, NULL, awaitVehicleRepairThread, 0);
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                    if (msg.resource_type == RESOURCE_SEA) { //REL dotyczył dostępu do morza
                        if (ok_count == size) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessTechnician(rank)) {
                                log(rank, l_clock, "Yay! Dostalem technika.");
                                current_state = STATE_REPAIR_IN_PROGRESS;
                                ok_count = 0;
                                // tworzymy nowy wątek oczekiwania na koniec naprawy
                                pthread_t threadA;
                                pthread_create( &threadA, NULL, awaitVehicleRepairThread, 0);
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }
                break;

            case STATE_REPAIR_IN_PROGRESS:
                // czekamy na koniec naprawy - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp)
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                if (msg_type == MSG_TYPE_REL)
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                if (msgtype == MSG_TYPE_REQ)
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                break;
        }       

        // zdejmujemy mutex
        pthread_mutex_unlock(&mut);
        // i wracamy do początku pętli oczekując na kolejne wiadomości

    }

    // TODO - ubrać poniższe w przechwytywanie sygnału Ctrl+C (?) -- nie mam pewności
    pthread_mutex_destroy(&mut);

    MPI_Finalize();
}