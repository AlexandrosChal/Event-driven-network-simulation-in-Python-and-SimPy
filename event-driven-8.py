import simpy
import matplotlib.pyplot as plt
import sys

# --- 1. Επιλογή Αλγορίθμου από τον Χρήστη ---
def get_user_choice():
    print("\n--- Επιλογή Αλγορίθμου Packet Scheduling ---")
    print("1. FCFS (First-Come-First-Serve)")
    print("2. RR (Round Robin)")
    
    while True:
        try:
            choice = input("Επίλεξε (1 ή 2): ").strip()
            if choice == '1':
                return 'FCFS'
            elif choice == '2':
                return 'RR'
            else:
                print("Λάθος επιλογή. Παρακαλώ δώσε 1 ή 2.")
        except KeyboardInterrupt:
            print("\nΈξοδος...")
            sys.exit()

# Ζητάμε την επιλογή πριν ξεκινήσουν οι παράμετροι
SCHEDULING_ALGORITHM = get_user_choice()

# --- 2. Παράμετροι & Σενάρια ---

# Σενάριο: 'MIXED' (Μικρά/Αραιά vs Μεγάλα/Συχνά)
SCENARIO = 'MIXED' 

BANDWIDTH_BPS = 1_000_000
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8
PROP_DELAY_LINK_S = 0.050

# ΠΡΟΣΟΧΗ: Ρυθμίσαμε το QUEUE_SIZE στο 20 συνολικά.
# Ο αλγόριθμος RR θα το μοιράσει (10+10) αυτόματα μέσα στην κλάση Router
# για να είναι δίκαιη η σύγκριση μνήμης.
QUEUE_SIZE = 20 

NUM_PACKETS_PER_HOST = 1000  # Πόσα πακέτα θα στείλει ο καθένας
SIM_TIME_S = 20.0
MONITOR_INTERVAL_S = 0.1

# --- Ρύθμιση Μεγεθών και Ρυθμών (Varied Rates) ---
if SCENARIO == 'MIXED':
    # Host A (VoIP/Gaming): Μικρά πακέτα, Αραιή ροή (Light Load)
    SIZE_HOST_A = 100       # Bytes
    INTERVAL_HOST_A = 0.020 # Στέλνει κάθε 20ms (50 πακέτα/sec)
    
    # Host B (File Download): Μεγάλα πακέτα, Πυκνή ροή (Heavy Load)
    SIZE_HOST_B = 3000      # Bytes
    INTERVAL_HOST_B = 0.002 # Στέλνει κάθε 2ms (500 πακέτα/sec) -> ΠΡΟΚΑΛΕΙ ΣΥΜΦΟΡΗΣΗ

# --- 3. Μοντέλο Πακέτου ---
class Packet:
    def __init__(self, time_created, size_bytes, packet_id, src_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id
        self.src_id = src_id 

    def __repr__(self):
        return f"Packet(src={self.src_id}, id={self.packet_id}, size={self.size_bytes})"

# --- 4. Κόμβοι Δικτύου ---

def source_host(env, src_name, num_packets, packet_interval, packet_size, link_to_router, packets_sent_stats):
    """Γεννήτρια πακέτων με συγκεκριμένο ρυθμό (interval)."""
    for i in range(num_packets):
        packet = Packet(env.now, packet_size, i, src_name)
        link_to_router.put(packet)
        packets_sent_stats['count'] += 1
        yield env.timeout(packet_interval)

class Router:
    def __init__(self, env, name, bandwidth_bytes_per_sec, prop_delay_s, queue_size, algorithm='FCFS'):
        self.env = env
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay = prop_delay_s
        self.algorithm = algorithm
        self.packets_dropped = 0
        
        # Δομές Ουρών με διαμοιρασμό μνήμης
        if self.algorithm == 'FCFS':
            # Μία κοινή ουρά με όλη τη χωρητικότητα
            self.queue = simpy.Store(env, capacity=queue_size)
        elif self.algorithm == 'RR':
            # Δύο ουρές με τη μισή χωρητικότητα η καθεμία (Δίκαιη σύγκριση πόρων)
            split_capacity = int(queue_size / 2)
            self.queues = {
                'HostA': simpy.Store(env, capacity=split_capacity),
                'HostB': simpy.Store(env, capacity=split_capacity)
            }
            self.rr_cycle = ['HostA', 'HostB'] 
            self.rr_index = 0
            self.packet_arrival_event = env.event()

    def receive_packets(self, link_input):
        while True:
            packet = yield link_input.get()
            self.env.process(self.handle_arrival(packet))

    def handle_arrival(self, packet):
        yield self.env.timeout(self.prop_delay)
        
        if self.algorithm == 'FCFS':
            if len(self.queue.items) < self.queue.capacity:
                self.queue.put(packet)
            else:
                self.packets_dropped += 1
                
        elif self.algorithm == 'RR':
            target_queue = self.queues[packet.src_id]
            if len(target_queue.items) < target_queue.capacity:
                target_queue.put(packet)
                if not self.packet_arrival_event.triggered:
                    self.packet_arrival_event.succeed()
                    self.packet_arrival_event = self.env.event()
            else:
                self.packets_dropped += 1

    def send_packets(self, link_output):
        while True:
            packet_to_send = None
            
            if self.algorithm == 'FCFS':
                packet_to_send = yield self.queue.get()
                
            elif self.algorithm == 'RR':
                start_index = self.rr_index
                found = False
                # Έλεγχος ουρών κυκλικά
                for _ in range(len(self.rr_cycle)):
                    current_host = self.rr_cycle[self.rr_index]
                    # Μετακίνηση δείκτη ΓΙΑ ΤΗΝ ΕΠΟΜΕΝΗ ΦΟΡΑ
                    self.rr_index = (self.rr_index + 1) % len(self.rr_cycle)
                    
                    if len(self.queues[current_host].items) > 0:
                        packet_to_send = yield self.queues[current_host].get()
                        found = True
                        break
                
                if not found:
                    yield self.packet_arrival_event
                    continue

            # Transmission
            transmission_delay = packet_to_send.size_bytes / self.bandwidth
            yield self.env.timeout(transmission_delay)
            link_output.put(packet_to_send)

def destination_host(env, link_from_last_router, prop_delay_last_link, delay_stats, received_stats):
    while True:
        packet = yield link_from_last_router.get()
        env.process(handle_dest_arrival(env, packet, prop_delay_last_link, delay_stats, received_stats))

def handle_dest_arrival(env, packet, prop_delay, delay_stats, received_stats):
    yield env.timeout(prop_delay)
    arrival_time = env.now
    delay = arrival_time - packet.time_created
    
    delay_stats.append((arrival_time, delay, packet.src_id))
    received_stats['count'] += 1
    received_stats['total_bits'] += packet.size_bytes * 8

# --- 5. Monitoring & Main ---
def monitor_throughput(env, interval, received_stats, throughput_stats):
    last_checked_bits = 0
    while True:
        yield env.timeout(interval)
        current_total = received_stats['total_bits']
        mbps = ((current_total - last_checked_bits) / interval) / 1e6
        throughput_stats.append((env.now, mbps))
        last_checked_bits = current_total

if __name__ == "__main__":
    delay_stats = [] 
    throughput_stats = []
    packets_sent_stats = {'count': 0}
    received_stats = {'count': 0, 'total_bits': 0}

    print(f"\n--- Starting Simulation: {SCHEDULING_ALGORITHM} ---")
    print(f"Host A (Light): {SIZE_HOST_A} B, κάθε {INTERVAL_HOST_A}s")
    print(f"Host B (Heavy): {SIZE_HOST_B} B, κάθε {INTERVAL_HOST_B}s")

    env = simpy.Environment()
    
    link_src_r1 = simpy.Store(env)
    link_r1_r2 = simpy.Store(env)
    link_r2_dest = simpy.Store(env)
    
    # Router 1: Aggregator με τον επιλεγμένο αλγόριθμο
    router1 = Router(env, "R1", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE, algorithm=SCHEDULING_ALGORITHM)
    # Router 2: Backbone (FCFS)
    router2 = Router(env, "R2", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE, algorithm='FCFS')
    
    # Δημιουργία κίνησης με διαφορετικούς ρυθμούς
    env.process(source_host(env, "HostA", NUM_PACKETS_PER_HOST, INTERVAL_HOST_A, SIZE_HOST_A, link_src_r1, packets_sent_stats))
    env.process(source_host(env, "HostB", NUM_PACKETS_PER_HOST, INTERVAL_HOST_B, SIZE_HOST_B, link_src_r1, packets_sent_stats))
    
    env.process(router1.receive_packets(link_src_r1))
    env.process(router1.send_packets(link_r1_r2))
    
    env.process(router2.receive_packets(link_r1_r2))
    env.process(router2.send_packets(link_r2_dest))
    
    env.process(destination_host(env, link_r2_dest, PROP_DELAY_LINK_S, delay_stats, received_stats))
    env.process(monitor_throughput(env, MONITOR_INTERVAL_S, received_stats, throughput_stats))
    
    env.run(until=SIM_TIME_S)

    # --- Plotting ---
    plt.figure(figsize=(14, 6))
    
    # 1. Delay per Host (Scatter)
    plt.subplot(1, 2, 1)
    # Φιλτράρισμα δεδομένων ανά Host
    times_a = [x[0] for x in delay_stats if x[2]=='HostA']
    delays_a = [x[1] for x in delay_stats if x[2]=='HostA']
    times_b = [x[0] for x in delay_stats if x[2]=='HostB']
    delays_b = [x[1] for x in delay_stats if x[2]=='HostB']
    
    plt.plot(times_a, delays_a, 'o', markersize=3, label='Host A (Light/Small)', color='blue', alpha=0.7)
    plt.plot(times_b, delays_b, 'x', markersize=3, label='Host B (Heavy/Large)', color='orange', alpha=0.4)
    
    plt.title(f"Delay per Packet ({SCHEDULING_ALGORITHM})")
    plt.xlabel("Time (s)")
    plt.ylabel("End-to-End Delay (s)")
    plt.legend()
    plt.grid(True)
    
    # 2. Throughput
    plt.subplot(1, 2, 2)
    t_time = [x[0] for x in throughput_stats]
    t_val = [x[1] for x in throughput_stats]
    plt.plot(t_time, t_val, color='green')
    plt.axhline(y=BANDWIDTH_BPS/1e6, color='red', linestyle='--', label='Max Link Capacity')
    plt.title("Total Network Throughput")
    plt.xlabel("Time (s)")
    plt.ylabel("Mbps")
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()