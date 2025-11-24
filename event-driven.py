import simpy
import matplotlib.pyplot as plt

# --- 1. Παράμετροι & Σενάρια ---

# Επιλογή Αλγορίθμου: 'FCFS' ή 'RR' (Round Robin)
SCHEDULING_ALGORITHM = 'RR'  

# Σενάριο Κυκλοφορίας: 'EQUAL' (ίδια πακέτα) ή 'MIXED' (Μικρά vs Μεγάλα)
SCENARIO = 'MIXED' 

BANDWIDTH_BPS = 1_000_000
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8
PACKET_INTERVAL_S = 0.005
PROP_DELAY_LINK_S = 0.050
QUEUE_SIZE = 20 # Αυξήσαμε λίγο την ουρά για να χωρέσουν τα πειράματα
NUM_PACKETS_TOTAL = 2000
SIM_TIME_S = 20.0
MONITOR_INTERVAL_S = 0.1

# Ρύθμιση Μεγεθών ανάλογα με το σενάριο
if SCENARIO == 'EQUAL':
    SIZE_HOST_A = 1000
    SIZE_HOST_B = 1000
elif SCENARIO == 'MIXED':
    SIZE_HOST_A = 100   # Μικρά πακέτα (π.χ. VoIP/Gaming)
    SIZE_HOST_B = 3000  # Μεγάλα πακέτα (π.χ. File Download)

# --- 2. Μοντέλο Πακέτου ---
class Packet:
    def __init__(self, time_created, size_bytes, packet_id, src_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id
        self.src_id = src_id 

    def __repr__(self):
        return f"Packet(src={self.src_id}, id={self.packet_id}, size={self.size_bytes})"

# --- 3. Κόμβοι Δικτύου ---

def source_host(env, src_name, num_packets, packet_interval, packet_size, link_to_router, packets_sent_stats):
    for i in range(num_packets):
        packet = Packet(env.now, packet_size, i, src_name)
        link_to_router.put(packet)
        packets_sent_stats['count'] += 1
        yield env.timeout(packet_interval)

class Router:
    def __init__(self, env, name, bandwidth_bytes_per_sec, prop_delay_s, queue_size, algorithm='FCFS'):
        self.env = env
        self.name = name
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay = prop_delay_s
        self.algorithm = algorithm
        self.queue_capacity = queue_size
        self.packets_dropped = 0
        
        # Δομές Ουρών
        if self.algorithm == 'FCFS':
            self.queue = simpy.Store(env, capacity=queue_size)
        elif self.algorithm == 'RR':
            # Ξεχωριστές ουρές για κάθε Host
            self.queues = {
                'HostA': simpy.Store(env, capacity=queue_size),
                'HostB': simpy.Store(env, capacity=queue_size)
            }
            self.rr_cycle = ['HostA', 'HostB'] # Σειρά εξυπηρέτησης
            self.rr_index = 0
            self.packet_arrival_event = env.event() # Event για ξύπνημα όταν έρθει πακέτο

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
                # Ειδοποιούμε την send_packets ότι ήρθε κάτι (αν κοιμάται)
                if not self.packet_arrival_event.triggered:
                    self.packet_arrival_event.succeed()
                    self.packet_arrival_event = self.env.event() # Reset event
            else:
                self.packets_dropped += 1

    def send_packets(self, link_output):
        while True:
            packet_to_send = None
            
            if self.algorithm == 'FCFS':
                packet_to_send = yield self.queue.get()
                
            elif self.algorithm == 'RR':
                # Έλεγχος ουρών κυκλικά (Round Robin)
                start_index = self.rr_index
                found = False
                
                # Κάνουμε έναν κύκλο για να βρούμε πακέτο
                for _ in range(len(self.rr_cycle)):
                    current_host = self.rr_cycle[self.rr_index]
                    # Μετακίνηση δείκτη για την επόμενη φορά (πριν τον έλεγχο, για δίκαιη σειρά)
                    self.rr_index = (self.rr_index + 1) % len(self.rr_cycle)
                    
                    if len(self.queues[current_host].items) > 0:
                        packet_to_send = yield self.queues[current_host].get()
                        found = True
                        break
                
                if not found:
                    # Αν όλες οι ουρές είναι άδειες, περίμενε να έρθει κάτι
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
    
    # Αποθήκευση delay ανά Host για σύγκριση
    delay_stats.append((arrival_time, delay, packet.src_id))
    received_stats['count'] += 1
    received_stats['total_bits'] += packet.size_bytes * 8
    received_stats['last_arrival_time'] = arrival_time

# --- 4. Monitoring & Main ---
# (Οι συναρτήσεις monitoring είναι ίδιες, παραλείπονται για συντομία, είναι ενσωματωμένες στο main)
def monitor_throughput(env, interval, received_stats, throughput_stats):
    last_checked_bits = 0
    while True:
        yield env.timeout(interval)
        current_total = received_stats['total_bits']
        mbps = ((current_total - last_checked_bits) / interval) / 1e6
        throughput_stats.append((env.now, mbps))
        last_checked_bits = current_total

if __name__ == "__main__":
    delay_stats = [] # (time, delay, src_id)
    throughput_stats = []
    packets_sent_stats = {'count': 0}
    received_stats = {'count': 0, 'total_bits': 0, 'last_arrival_time': 0}

    print(f"--- Simulation: {SCHEDULING_ALGORITHM} | Scenario: {SCENARIO} ---")
    print(f"Host A Size: {SIZE_HOST_A} B | Host B Size: {SIZE_HOST_B} B")

    env = simpy.Environment()
    
    link_src_r1 = simpy.Store(env)
    link_r1_r2 = simpy.Store(env)
    link_r2_dest = simpy.Store(env)
    
    # Router 1 εφαρμόζει τον αλγόριθμο (Aggregator)
    router1 = Router(env, "R1", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE, algorithm=SCHEDULING_ALGORITHM)
    # Router 2 είναι απλός FIFO (Backbone)
    router2 = Router(env, "R2", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE, algorithm='FCFS')
    
    packets_per_host = NUM_PACKETS_TOTAL // 2
    env.process(source_host(env, "HostA", packets_per_host, PACKET_INTERVAL_S, SIZE_HOST_A, link_src_r1, packets_sent_stats))
    env.process(source_host(env, "HostB", packets_per_host, PACKET_INTERVAL_S, SIZE_HOST_B, link_src_r1, packets_sent_stats))
    
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
    times_a = [x[0] for x in delay_stats if x[2]=='HostA']
    delays_a = [x[1] for x in delay_stats if x[2]=='HostA']
    times_b = [x[0] for x in delay_stats if x[2]=='HostB']
    delays_b = [x[1] for x in delay_stats if x[2]=='HostB']
    
    plt.plot(times_a, delays_a, 'o', markersize=2, label='Host A (Small)', alpha=0.6)
    plt.plot(times_b, delays_b, 'x', markersize=2, label='Host B (Large)', alpha=0.6)
    plt.title(f"Delay per Packet ({SCHEDULING_ALGORITHM} - {SCENARIO})")
    plt.xlabel("Time (s)")
    plt.ylabel("Delay (s)")
    plt.legend()

    plt.grid(True)
    
    # 2. Throughput
    plt.subplot(1, 2, 2)
    t_time = [x[0] for x in throughput_stats]
    t_val = [x[1] for x in throughput_stats]
    plt.plot(t_time, t_val, color='green')
    plt.axhline(y=BANDWIDTH_BPS/1e6, color='red', linestyle='--', label='Max Limit')
    plt.title("Total Throughput")
    plt.xlabel("Time (s)")
    plt.ylabel("Mbps")
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()