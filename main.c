#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

/* wątki */
#include <pthread.h>

/* stałe dot zasobów */
#define SEA_SLOTS 10 // maksymalna liczba turystów na morzu
#define VEHICLE_SLOTS 3 // liczba pojazdów
#define TECHNICIAN_SLOTS 2 // liczba techników
#define VEHICLE_STARTING_HEALTH 10 // maksymalna wartość stanu technicznego pojazdu
#define VEHICLE_HEALTH_MAX_DECLINE 5 // maksymalna wartość ubytku stanu technicznego pojazdu po jednej wycieczce
#define MAX_TOURISTS 8 // maksymalna liczba wylosowanych turystów

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

//liczba turystów
int tourists = 0;

// kolejka do morza
typedef struct sea_queue_elT {
    int client_id;
    int count;
    int timestamp;
    struct sea_queue_elT *next;
} sea_queue_el;

sea_queue_el *sea_queue=NULL;

// tablica kolejek do pojazdów
typedef struct vehicle_queue_elT {
    int client_id;
    int timestamp;
    struct vehicle_queue_elT *next;
} vehicle_queue_el;

vehicle_queue_el *vehicle_queue[VEHICLE_SLOTS];

// kolejka do techników
typedef struct technician_queue_elT {
    int client_id;
    int timestamp;
    struct technician_queue_elT *next;
} technician_queue_el;

technician_queue_el *technician_queue = NULL;

// tablica stanów pojazdów
int vehicle_health[VEHICLE_SLOTS];

// lista numerów pojazdów w kolejce do których stoimy
// TODO
// na początek uproszczone - jednocześnie stoimy w kolejce do jednego pojazdu
int awaited_vehicle_id = -1;

int size, rank;

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
        if (i != rank) {
            MPI_Send(&msg, 3, MPI_INT, i, MSG_TYPE_REQ, MPI_COMM_WORLD);
        }
    }
}

void sendREL(int resource_type, int resource_id, int resource_update_value, int source, int count) {

    msg_REL msg;
    msg.resource_id = resource_id;
    msg.resource_type = resource_type;
    msg.resource_update_value = resource_update_value;
    msg.timestamp = ++l_clock;

    for (int i = 0; i < count; i++) {
        if (i != rank) {
            MPI_Send(&msg, 4, MPI_INT, i, MSG_TYPE_REL, MPI_COMM_WORLD);
        } 
    }
}

void sendOK(int target) {

    msg_OK msg;
    msg.timestamp = ++l_clock;

    MPI_Send(&msg, 2, MPI_INT, target, MSG_TYPE_OK, MPI_COMM_WORLD);
}

// funkcja logująca (w konsoli)

void p_log(int id, int timestamp, char *message) {
    printf("[P%02d][t=%05d] %s\n", id, timestamp, message);
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
            if (s <= SEA_SLOTS) {
                printf("[P%02d][t=%05d] Moge wejsc na morze, z moimi pasazerami na morzu bedzie przynajmniej %d osob\n", rank, l_clock, s);
                return TRUE;
            }
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
    if (vehicle_queue[vehicle_id]->client_id == rank) {
        int l = 0;
        vehicle_queue_el *cur = vehicle_queue[vehicle_id];
        while (cur != NULL) {
            cur = cur->next;
            l++;
        }
        printf("[P%02d][t=%05d] Moge zabrac pojazd %d, dlugosc kolejki = %d \n", rank, l_clock, vehicle_id, l);
        return TRUE;
    } else {
        return FALSE;
    }
}

// --- czy można uzyskać dostęp do technika
int canAccessTechnician(int rank)
{
    technician_queue_el *cur = technician_queue;
    int i = 1;
    while (cur != NULL) {
        if (cur->client_id == rank) {
            if (i <= TECHNICIAN_SLOTS) {
                printf("[P%02d][t=%05d] Dostalem technika bedac w top%d kolejki do technikow\n", rank, l_clock, i);
                return TRUE;
            }
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
        // preferujemy pojazdy o id odpowiadajacemu naszemu procesowi
        if (l < shortest_len || (l == shortest_len && (i % size == rank))) {
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
        case 1: { //resource_sea
            sea_queue_el *prev = NULL;
            sea_queue_el *sq = sea_queue;
            while (sq != NULL) {
                if (sq->client_id == client_id) {
                    if (prev == NULL) {
                        sea_queue = sq->next;
                        free(sq);
                        return;
                    } else {
                        prev->next = sq->next;
                        free(sq);
                        return;
                    }
                }
                prev = sq;
                sq = sq->next;
            }           
            }
            break;
        case 2: { //resource_vehicle
            vehicle_queue_el *prev_vehicle = NULL;
            vehicle_queue_el *vq = vehicle_queue[resource_id];
            if (resource_update_value > 0) {
                vehicle_health[resource_id] = resource_update_value;
            } 
            while(vq != NULL) {
                if (vq->client_id == client_id) {
                    if (prev_vehicle == NULL) {
                        vehicle_queue[resource_id] = vq->next;
                        free(vq);
                        return;
                    } else {
                        prev_vehicle->next = vq->next;
                        free(vq);
                        return;
                    }
                }
                prev_vehicle = vq;
                vq = vq->next;
            }  
            }        
            break;
        case 3: {// resource_technician
            technician_queue_el *prev_tech = NULL;
            technician_queue_el *tq = technician_queue;
            while (tq != NULL) {
                if (tq->client_id == client_id) {
                    if (prev_tech == NULL) {
                        technician_queue = tq->next;
                        free(tq);
                        return;
                    } else {
                        prev_tech->next = tq->next;
                        free(tq);
                        return;
                    }
                }
                prev_tech = tq;
                tq = tq->next;
            }
            }
            break;
    }
}

// aktualizuje kolejkę w oparciu o REQ danego typu - dodaje element do kolejki danego zasobu
void updateReq(int resource_type, int resource_id, int timestamp, int client_id)
{
    switch(resource_type) {
        case RESOURCE_SEA: {
            sea_queue_el *n = malloc(sizeof(*n));
            n->client_id = client_id;
            n->timestamp = timestamp;
            n->count = resource_id;

            sea_queue_el *prev = NULL;
            sea_queue_el *sq = sea_queue;

            if (sea_queue == NULL) {
                sea_queue = n;
                n->next = NULL;
                return;
            }

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
            }     
            break;
        case RESOURCE_VEHICLE: {
            vehicle_queue_el *nv = malloc(sizeof(*nv));
            nv->client_id = client_id;
            nv->timestamp = timestamp;

            if (vehicle_queue[resource_id] == NULL) {
                vehicle_queue[resource_id] = nv;
                nv->next = NULL;
                return;
            }
            

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
            }         
            break;
        case RESOURCE_TECHNICIAN: {
            technician_queue_el *nt = malloc(sizeof(*nt));
            nt->timestamp = timestamp;
            nt->client_id = client_id;

            if (technician_queue == NULL) {
                technician_queue = nt;
                nt->next = NULL;
                return;
            }

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
                prev_tech->next = nt;
                nt->next = NULL;
            }
            }
            break;
    }
}

/* TWORZONE WĄTKI */

// wątek oczekujący na turystów
void *awaitTouristsThread(void *ptr)
{
    p_log(rank, l_clock, "Czekam na turystow...");

    // losowy sleep 1s do 3s
    usleep(rand() % 2000000 + 1000000);

    // wylosuj liczbę użytkowników
    tourists = rand() % MAX_TOURISTS + 1;

    printf("[P%02d][t=%05d] Zglosilo sie do mnie %d turystow, poprosze o dostep do morza \n", rank, l_clock, tourists);

    // mutex on
    pthread_mutex_lock(&mut);

    // wyślij REQ do morza
    ok_count = 0;
    updateReq(RESOURCE_SEA, tourists, l_clock+1, rank);
    sendREQ(RESOURCE_SEA, tourists, rank, size);

    // zmień stan na 2 (czekamy na dostęp do morza)
    current_state = STATE_AWAIT_SEA_ACCESS;
    ok_count = 0;

    // mutex off
    pthread_mutex_unlock(&mut);
}

// wątek oczekujący na zakończenie wycieczki
void *awaitTourEndThread(void *ptr)
{
    // losowy sleep 1s do 3s
    usleep(rand() % 2000000 + 1000000);

    // mutex on
    pthread_mutex_lock(&mut);

    // wylosuj spadek stanu technicznego pojazdu
    int damage = rand() % VEHICLE_HEALTH_MAX_DECLINE + 1;
    int new_health = vehicle_health[current_vehicle_id] - damage;

    // jeśli nowy stan techniczny > 0
    if (new_health > 0) {
        printf("[P%02d][t=%05d] Koniec wycieczki, pojazd ok, nowy stan pojazdu nr %d to %d, zwalniam pojazd i morze \n", rank, l_clock, current_vehicle_id, new_health);

        updateRel(RESOURCE_VEHICLE, current_vehicle_id, new_health, rank);
        // wyślij REL dla pojazdu
        sendREL(RESOURCE_VEHICLE, current_vehicle_id, new_health, rank, size);
        
        updateRel(RESOURCE_SEA, 0, 0, rank);
        // wyślij REL dla morza
        sendREL(RESOURCE_SEA, 0, 0, rank, size);

        // przejdź do stanu 1 - oczekuj na turystów
        current_state = STATE_AWAIT_TOURISTS;

        current_vehicle_id = -1;

        // tworzymy wątek czekający na turystów
        pthread_t threadStart;
        pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    }
    else
    {
        printf("[P%02d][t=%05d] Koniec wycieczki, pojazd nr %d zepsuty, prosze o technika \n", rank, l_clock, current_vehicle_id);
        // wpp - jeśli pojazd się popsuł
        // wyślij REQ dla technika
        ok_count = 0;
        updateReq(RESOURCE_TECHNICIAN, 0, l_clock+1, rank);
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


    printf("[P%02d][t=%05d] Koniec naprawy pojazdu nr %d, zwalniam technika, pojazd i morze \n", rank, l_clock, current_vehicle_id);

    // mutex on
    pthread_mutex_lock(&mut);
    
    updateRel(RESOURCE_TECHNICIAN, 0, 0, rank);
    // REL dla technika
    sendREL(RESOURCE_TECHNICIAN, 0, 0, rank, size);

    updateRel(RESOURCE_VEHICLE, current_vehicle_id, VEHICLE_STARTING_HEALTH, rank);
    // REL dla pojazdu z MAX stanem technicznym
    sendREL(RESOURCE_VEHICLE, current_vehicle_id, VEHICLE_STARTING_HEALTH, rank, size);

    updateRel(RESOURCE_SEA, 0, 0, rank);
    // REL dla morza
    sendREL(RESOURCE_SEA, 0, 0, rank, size);

    // przejdź do stanu 1 - oczekuj na turystów
    current_state = STATE_AWAIT_TOURISTS;

    current_vehicle_id = -1;
    
    // tworzymy wątek czekający na turystów
    pthread_t threadStart;
    pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    // mutex off
    pthread_mutex_unlock(&mut);
}


// główna funkcja
int main(int argc, char **argv)
{

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
        vehicle_queue[i] = NULL;
    }

    MPI_Status status;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(0) + rank);


    // tworzymy wątek oczekujący na zgłoszenie się turystów
    pthread_t threadStart;
    pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    // główna pętla

    while(TRUE) {

        // czekamy na wiadomości
        msg_REL msg;
        MPI_Recv( &msg, 4, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        
        int msg_type = status.MPI_TAG;
        int sender = status.MPI_SOURCE;

        //printf("[P%02d] Odebrano wiadomosc typu %d\n", rank, msg_type);

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
            case STATE_AWAIT_TOURISTS: {
                // czekamy na turystów - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                } 
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }     
                }
                break;

            case STATE_AWAIT_SEA_ACCESS: {
                // oczekujemy na dostęp do morza
                // tutaj chcemy zliczać liczbę OKejek które dostajemy
                // i jeśli jest ona dostatecznie wysoka - wchodzimy
                if (msg_type == MSG_TYPE_OK) {
                    if (msg.timestamp > req_timestamp) {
                        ok_count++;
                        if (ok_count == size - 1) { //już wszystkie procesy zaktualizowały informacje
                            if (canAccessSea(rank)) {
                                current_state = STATE_AWAIT_VEHICLE_ACCESS;
                                ok_count = 0;
                                int best_vehicle = findShortestVehicleQueue(rank);
                                awaited_vehicle_id = best_vehicle;
                                printf("[P%02d][t=%05d] Mam juz dostep do morza, prosze o pojazd nr %d\n", rank, l_clock, best_vehicle);
                                updateReq(RESOURCE_VEHICLE, best_vehicle, l_clock+1, rank);
                                sendREQ(RESOURCE_VEHICLE, best_vehicle, rank, size);
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                    if (msg.resource_type == RESOURCE_SEA) { //REL dotyczył dostępu do morza
                        if (ok_count == size - 1) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessSea(rank)) {
                                current_state = STATE_AWAIT_VEHICLE_ACCESS;
                                ok_count = 0;
                                int best_vehicle = findShortestVehicleQueue(rank);
                                awaited_vehicle_id = best_vehicle;
                                printf("[P%02d][t=%05d] Mam juz dostep do morza, prosze o pojazd nr %d\n", rank, l_clock, best_vehicle);
                                updateReq(RESOURCE_VEHICLE, best_vehicle, l_clock+1, rank);
                                sendREQ(RESOURCE_VEHICLE, best_vehicle, rank, size);
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }
            }
                break;

            case STATE_AWAIT_VEHICLE_ACCESS: {
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
                        if (ok_count == size - 1) { // jeśli mamy komplet OK
                            if (canAccessVehicle(rank, awaited_vehicle_id)) { //jeśli możemy wejść do pojazdu to wchodzimy
                                current_vehicle_id = awaited_vehicle_id;
                                awaited_vehicle_id = -1;
                                current_state = STATE_TOUR_IN_PROGRESS;
                                ok_count = 0;
                                printf("[P%02d][t=%05d] Dostalem pojazd nr %d (health = %d), rozpoczynam wycieczke\n", rank, l_clock, current_vehicle_id, vehicle_health[current_vehicle_id]);
                                pthread_t threadTour;
                                pthread_create( &threadTour, NULL, awaitTourEndThread, 0);                                
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) { // jeśli odbieramy rel
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender); // to aktualizujemy kolejkę
                    if (msg.resource_type == RESOURCE_VEHICLE && ok_count == size - 1 && msg.resource_id == awaited_vehicle_id) { // i jeśli rel dotyzcył pojazdu w kolejce
                    //  do którego stoimy
                        if (canAccessVehicle(rank, awaited_vehicle_id)) { // to próbujemy do niego wejść (jeśli możemy)
                            current_vehicle_id = awaited_vehicle_id;
                            awaited_vehicle_id = -1;
                            current_state = STATE_TOUR_IN_PROGRESS;
                            ok_count = 0;
                            printf("[P%02d][t=%05d] Dostalem pojazd nr %d (health = %d), rozpoczynam wycieczke\n", rank, l_clock, current_vehicle_id, vehicle_health[current_vehicle_id]);
                            pthread_t threadTour2;
                            pthread_create( &threadTour2, NULL, awaitTourEndThread, 0);  
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) { // jeśli to req, to aktualizujemy kolejkę i odsyłamy OKejkę
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }
            }
                break;

            case STATE_TOUR_IN_PROGRESS: {
                // czekamy na koniec wycieczki - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                }  
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender);
                }  
            }
                break;
            
            case STATE_AWAIT_TECHNICIAN: {
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++;
                    if (ok_count == size - 1) { //już wszystkie procesy zaktualizowały informacje
                        if (canAccessTechnician(rank)) {
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
                        if (ok_count == size - 1) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessTechnician(rank)) {
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
            }
                break;

            case STATE_REPAIR_IN_PROGRESS: {
                // czekamy na koniec naprawy - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK && msg.timestamp > req_timestamp) {
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value, sender);
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id, msg.timestamp, sender);
                    sendOK(sender); //zdradzieckie ify bez bloku z wąsów {} - 30min debuggowania
                }
                break;
            }
        }       

        // zdejmujemy mutex
        pthread_mutex_unlock(&mut);
        // i wracamy do początku pętli oczekując na kolejne wiadomości

    }

    // TODO - ubrać poniższe w przechwytywanie sygnału Ctrl+C (?) -- nie mam pewności
    pthread_mutex_destroy(&mut);

    MPI_Finalize();
}