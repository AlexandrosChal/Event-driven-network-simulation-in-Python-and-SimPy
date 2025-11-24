import simpy
import matplotlib.pyplot as plt

# --- 1. Παράμετροι Προσομοίωσης ---

# Χρησιμοποιούμε 1 Mbps για να δούμε καθαρά τη συμφόρηση
BANDWIDTH_BPS = 1_000_000
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8

PACKET_SIZE_BYTES = 5000 
PACKET_INTERVAL_S = 0.005  # Κάθε πηγή στέλνει με αυτόν τον ρυθμό

# Καθυστέρηση διάδοσης
PROP_DELAY_LINK_S = 0.050  

QUEUE_SIZE = 10
NUM_PACKETS_TOTAL = 2000 # Συνολικά πακέτα στο δίκτυο
SIM_TIME_S = 20.0
MONITOR_INTERVAL_S = 0.1

# --- 2. Μοντέλο Πακέτου ---
class Packet:
    def __init__(self, time_created, size_bytes, packet_id, src_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id
        self.src_id = src_id # Από ποιον host ήρθε (A ή B)

    def __repr__(self):
        return f"Packet(src={self.src_id}, id={self.packet_id})"

# --- 3. Κόμβοι Δικτύου ---

def source_host(env, src_name, num_packets, packet_interval, packet_size, link_to_router, packets_sent_stats):
    """
    Γενική συνάρτηση για Host. Θα την καλέσουμε 2 φορές (για Host A και Host B).
    """
    for i in range(num_packets):
        packet = Packet(
            time_created=env.now,
            size_bytes=packet_size,
            packet_id=i,
            src_id=src_name
        )
        link_to_router.put(packet)
        packets_sent_stats['count'] += 1
        yield env.timeout(packet_interval)

class Router:
    def __init__(self, env, name, bandwidth_bytes_per_sec, prop_delay_s, queue_size):
        self.env = env
        self.name = name
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay = prop_delay_s
        self.queue = simpy.Store(env, capacity=queue_size)
        self.packets_dropped = 0

    def receive_packets(self, link_input):
        """Λαμβάνει πακέτα (Shared Link αν είναι ο R1)."""
        while True:
            packet = yield link_input.get()
            self.env.process(self.handle_arrival(packet))

    def handle_arrival(self, packet):
        # Propagation Delay (εισερχόμενο)
        yield self.env.timeout(self.prop_delay)
        
        # Έλεγχος ουράς
        if len(self.queue.items) < self.queue.capacity:
            self.queue.put(packet)
        else:
            self.packets_dropped += 1

    def send_packets(self, link_output):
        while True:
            packet = yield self.queue.get()

            # Transmission Delay
            transmission_delay = packet.size_bytes / self.bandwidth
            yield self.env.timeout(transmission_delay)

            # Forwarding: Απλά βάζουμε το πακέτο στο επόμενο link
            # Το propagation delay θα το χρεωθεί ο επόμενος κόμβος ή ο προορισμός
            link_output.put(packet)


# --- ΔΙΟΡΘΩΣΗ: Νέα λογική Destination Host με Pipelining ---
def destination_host(env, link_from_last_router, prop_delay_last_link, delay_stats, received_stats):
    while True:
        packet = yield link_from_last_router.get()
        # Ξεκινάμε νέα διεργασία για κάθε πακέτο ώστε να μην μπλοκάρει η λήψη του επόμενου
        env.process(handle_dest_arrival(env, packet, prop_delay_last_link, delay_stats, received_stats))

def handle_dest_arrival(env, packet, prop_delay, delay_stats, received_stats):
    # Προσομοίωση Propagation Delay στο τελευταίο καλώδιο
    yield env.timeout(prop_delay)
    
    arrival_time = env.now
    end_to_end_delay = arrival_time - packet.time_created
    
    delay_stats.append((arrival_time, end_to_end_delay))
    received_stats['count'] += 1
    received_stats['total_bits'] += packet.size_bytes * 8
    received_stats['last_arrival_time'] = arrival_time
# -----------------------------------------------------------


# --- 4. Monitoring ---

def monitor_queue_size(env, router, interval, queue_size_stats):
    while True:
        current_queue_size = len(router.queue.items)
        queue_size_stats.append((env.now, current_queue_size))
        yield env.timeout(interval)

def monitor_throughput(env, interval, received_stats, throughput_stats):
    last_checked_bits = 0
    while True:
        yield env.timeout(interval)
        current_total_bits = received_stats['total_bits']
        bits_since_last_check = current_total_bits - last_checked_bits
        
        current_throughput_mbps = (bits_since_last_check / interval) / 1_000_000
        throughput_stats.append((env.now, current_throughput_mbps))
        last_checked_bits = current_total_bits
        
def monitor_total_packet_loss(env, interval, routers_list, packets_sent_stats, loss_rate_stats):
    while True:
        yield env.timeout(interval)
        total_sent = packets_sent_stats['count']
        total_dropped = sum(r.packets_dropped for r in routers_list)
        
        if total_sent > 0:
            loss_rate = (total_dropped / total_sent) * 100
        else:
            loss_rate = 0
        loss_rate_stats.append((env.now, loss_rate))

# --- 5. Κύριο Πρόγραμμα (Main) ---
if __name__ == "__main__":
    delay_stats = []
    queue_size_stats_r1 = [] 
    queue_size_stats_r2 = []
    throughput_stats = []
    loss_rate_stats = []
    
    packets_sent_stats = {'count': 0}
    received_stats = {'count': 0, 'total_bits': 0, 'last_arrival_time': 0}

    print(f"--- Έναρξη Προσομοίωσης: 2 Hosts -> Router 1 -> Router 2 -> Dest ---")
    print(f"Bandwidth: {BANDWIDTH_BPS/1e6} Mbps")
    print(f"Host A: 1000 πακέτα, Host B: 1000 πακέτα (Σύνολο: {NUM_PACKETS_TOTAL})")
    
    env = simpy.Environment()
    
    # Links
    # Κοινό link για τους δύο hosts προς τον R1
    link_sources_to_r1 = simpy.Store(env)
    link_r1_to_r2 = simpy.Store(env)
    link_r2_to_dest = simpy.Store(env)
    
    # Routers
    router1 = Router(env, "R1", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE)
    router2 = Router(env, "R2", BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK_S, QUEUE_SIZE)
    
    # --- Processes ---
    
    # 1. Δύο Hosts στέλνουν ταυτόχρονα στον Router 1
    # Μοιράζουμε τα πακέτα δια 2
    packets_per_host = NUM_PACKETS_TOTAL // 2
    
    env.process(source_host(env, "HostA", packets_per_host, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_sources_to_r1, packets_sent_stats))
    env.process(source_host(env, "HostB", packets_per_host, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_sources_to_r1, packets_sent_stats))
    
    # 2. Router 1 (Merge)
    env.process(router1.receive_packets(link_sources_to_r1))
    env.process(router1.send_packets(link_r1_to_r2))
    
    # 3. Router 2 (Forward)
    env.process(router2.receive_packets(link_r1_to_r2))
    env.process(router2.send_packets(link_r2_to_dest))
    
    # 4. Destination
    env.process(destination_host(env, link_r2_to_dest, PROP_DELAY_LINK_S, delay_stats, received_stats))
    
    # --- Monitors ---
    env.process(monitor_queue_size(env, router1, 0.01, queue_size_stats_r1))
    env.process(monitor_queue_size(env, router2, 0.01, queue_size_stats_r2))
    env.process(monitor_throughput(env, MONITOR_INTERVAL_S, received_stats, throughput_stats))
    env.process(monitor_total_packet_loss(env, MONITOR_INTERVAL_S, [router1, router2], packets_sent_stats, loss_rate_stats))
    
    env.run(until=SIM_TIME_S)
    
    print("\n--- Λήξη Προσομοίωσης ---")

    # Συνολικά Αποτελέσματα
    total_packets_sent = packets_sent_stats['count']
    total_dropped = router1.packets_dropped + router2.packets_dropped
    total_packets_received = received_stats['count']
    
    final_packet_loss_rate = (total_dropped / total_packets_sent) * 100 if total_packets_sent > 0 else 0
    
    total_time = received_stats.get('last_arrival_time', 0)
    total_bits_received = received_stats['total_bits']
    average_throughput_bps = total_bits_received / total_time if total_time > 0 else 0

    print("\n--- Τελικά Αποτελέσματα Μετρικών ---")
    print(f"Συνολικά Drops (R1+R2): {total_dropped}")
    print(f"Drops στον R1 (Aggregator): {router1.packets_dropped}")
    print(f"Drops στον R2 (Backbone): {router2.packets_dropped}")
    print(f"Τελικό Ποσοστό Απώλειας: {final_packet_loss_rate:.2f}%")
    print(f"Μέσο Throughput: {average_throughput_bps / 1_000_000:.4f} Mbps")

    # Plotting
    plt.figure(figsize=(16, 12))

    # 1. Delay
    plt.subplot(2, 2, 1)
    if delay_stats:
        x_val = [item[0] for item in delay_stats]
        y_val = [item[1] for item in delay_stats]
        plt.plot(x_val, y_val, marker='.', linestyle='None', markersize=1, alpha=0.5)
    plt.title("End-to-End Delay (Complex Network)", fontsize=14)
    plt.xlabel("Χρόνος Άφιξης (s)")
    plt.ylabel("Καθυστέρηση (s)")
    plt.grid(True)

    # 2. Queue Size Comparisson
    plt.subplot(2, 2, 2)
    if queue_size_stats_r1:
        x1 = [item[0] for item in queue_size_stats_r1]
        y1 = [item[1] for item in queue_size_stats_r1]
        plt.step(x1, y1, where='post', color='orange', label='Router 1 (Merge)')
    
    if queue_size_stats_r2:
        x2 = [item[0] for item in queue_size_stats_r2]
        y2 = [item[1] for item in queue_size_stats_r2]
        plt.step(x2, y2, where='post', color='blue', alpha=0.7, label='Router 2')
        
    plt.axhline(y=QUEUE_SIZE, color='r', linestyle='--', label='Max Capacity')
    plt.title("Σύγκριση Ουρών Routers", fontsize=14)
    plt.xlabel("Χρόνος (s)")
    plt.ylabel("Πακέτα")
    plt.legend()
    plt.grid(True)

    # 3. Throughput
    plt.subplot(2, 2, 3)
    if throughput_stats:
        x_val = [item[0] for item in throughput_stats]
        y_val = [item[1] for item in throughput_stats]
        plt.plot(x_val, y_val, color='green')
        plt.axhline(y=BANDWIDTH_BPS/1e6, color='black', linestyle='--', alpha=0.5, label='Max Link Bandwidth')
    plt.title("Throughput (Mbps)", fontsize=14)
    plt.xlabel("Χρόνος (s)")
    plt.ylabel("Mbps")
    plt.legend()
    plt.grid(True)

    # 4. Loss Rate
    plt.subplot(2, 2, 4)
    if loss_rate_stats:
        x_val = [item[0] for item in loss_rate_stats]
        y_val = [item[1] for item in loss_rate_stats]
        plt.plot(x_val, y_val, color='purple')
    plt.title("Αθροιστικό Ποσοστό Απώλειας Πακέτων (%)", fontsize=14)
    plt.xlabel("Χρόνος (s)")
    plt.ylabel("% Απώλειας")
    plt.grid(True)
    
    plt.tight_layout(pad=3.0)
    plt.show()