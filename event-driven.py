import simpy
import matplotlib.pyplot as plt

# --- 1. Παράμετροι Προσομοίωσης ---

# Bandwidth σε bits ανά δευτερόλεπτο (bps). 1 Mbps = 1,000,000 bps.
BANDWIDTH_BPS = 100_000_000
# Μετατροπή σε bytes/sec
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8

PACKET_SIZE_BYTES = 1000  # Μέγεθος πακέτου (1 KB)

# Ρυθμός άφιξης: 1 πακέτο κάθε 0.005s (200 πακέτα/sec)
PACKET_INTERVAL_S = 0.005

# Καθυστέρηση διάδοσης (Propagation Delay)
PROP_DELAY_LINK1_S = 0.050  # Source -> Router
PROP_DELAY_LINK2_S = 0.050  # Router -> Destination

# Μέγεθος της ουράς του router
QUEUE_SIZE = 10

# Αριθμός πακέτων (2000 πακέτα)
NUM_PACKETS = 2000

# Χρόνος προσομοίωσης (Αυξημένος για να χωρέσουν τα 2000 πακέτα)
SIM_TIME_S = 20.0

# Ρυθμός δειγματοληψίας monitors
MONITOR_INTERVAL_S = 0.1


# --- 2. Μοντέλο Πακέτου ---
class Packet:
    def __init__(self, time_created, size_bytes, packet_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id

    def __repr__(self):
        return f"Packet(id={self.packet_id}, size={self.size_bytes} B)"


# --- 3. Κόμβοι Δικτύου ---

def source_host(env, num_packets, packet_interval, packet_size, link_to_router, packets_sent_stats):
    """
    Δημιουργεί και στέλνει πακέτα στον Router.
    """
    for packet_id_counter in range(num_packets):
        packet = Packet(
            time_created=env.now,
            size_bytes=packet_size,
            packet_id=packet_id_counter
        )
        link_to_router.put(packet)
        packets_sent_stats['count'] += 1
        yield env.timeout(packet_interval)


class Router:
    def __init__(self, env, bandwidth_bytes_per_sec, prop_delay_link1_s, prop_delay_link2_s, queue_size):
        self.env = env
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay_link1 = prop_delay_link1_s
        self.prop_delay_link2 = prop_delay_link2_s
        self.queue = simpy.Store(env, capacity=queue_size)
        self.packets_dropped = 0

    def receive_packets(self, link_from_source):
        """
        Λαμβάνει πακέτα από το Source. 
        Προσομοιώνει το Propagation Delay της 1ης σύνδεσης παράλληλα για κάθε πακέτο.
        """
        while True:
            packet = yield link_from_source.get()
            self.env.process(self.handle_arrival(packet))

    def handle_arrival(self, packet):
        # Καθυστέρηση διάδοσης (Link 1)
        yield self.env.timeout(self.prop_delay_link1)
        
        # Έλεγχος ουράς
        if len(self.queue.items) < self.queue.capacity:
            self.queue.put(packet)
        else:
            self.packets_dropped += 1

    def send_packets(self, link_to_dest):
        """
        Εξυπηρετεί την ουρά και στέλνει στον προορισμό.
        Διαχωρισμός Transmission από Propagation για σωστό Throughput (Pipeline).
        """
        while True:
            # 1. Λήψη πακέτου από την ουρά
            packet = yield self.queue.get()

            # 2. Χρόνος Εκπομπής (Transmission Delay)
            # Ο Router απασχολείται ΜΟΝΟ για αυτό το χρονικό διάστημα
            transmission_delay = packet.size_bytes / self.bandwidth
            yield self.env.timeout(transmission_delay)

            # 3. Το πακέτο φεύγει από τον Router και ταξιδεύει (Link 2 Propagation).
            # Ξεκινάμε νέα διεργασία ώστε ο Router να είναι ελεύθερος για το επόμενο πακέτο.
            self.env.process(self.propagate_output(link_to_dest, packet))

    def propagate_output(self, link_to_dest, packet):
        """
        Βοηθητική διαδικασία που προσομοιώνει το ταξίδι του πακέτου στο καλώδιο.
        """
        yield self.env.timeout(self.prop_delay_link2)
        link_to_dest.put(packet)


def destination_host(env, link_from_router, delay_stats, received_stats):
    """
    Παραλαμβάνει τα πακέτα και καταγράφει στατιστικά.
    """
    while True:
        packet = yield link_from_router.get()
        
        arrival_time = env.now
        end_to_end_delay = arrival_time - packet.time_created
        
        delay_stats.append((arrival_time, end_to_end_delay))
        received_stats['count'] += 1
        received_stats['total_bits'] += packet.size_bytes * 8
        received_stats['last_arrival_time'] = arrival_time


# --- 4. Monitoring ---

def monitor_queue_size(env, router, interval, queue_size_stats):
    while True:
        # Εδώ χρειαζόμαστε το αντικείμενο router για να δούμε την ουρά του
        current_queue_size = len(router.queue.items)
        queue_size_stats.append((env.now, current_queue_size))
        yield env.timeout(interval)

def monitor_throughput(env, interval, received_stats, throughput_stats):
    last_checked_bits = 0
    while True:
        yield env.timeout(interval)
        current_total_bits = received_stats['total_bits']
        bits_since_last_check = current_total_bits - last_checked_bits
        
        # Mbps Calculation
        current_throughput_mbps = (bits_since_last_check / interval) / 1_000_000
        throughput_stats.append((env.now, current_throughput_mbps))
        last_checked_bits = current_total_bits
        
def monitor_packet_loss(env, interval, router, packets_sent_stats, loss_rate_stats):
    while True:
        yield env.timeout(interval)
        total_sent = packets_sent_stats['count']
        total_dropped = router.packets_dropped
        
        if total_sent > 0:
            loss_rate = (total_dropped / total_sent) * 100
        else:
            loss_rate = 0
        loss_rate_stats.append((env.now, loss_rate))


# --- 5. Κύριο Πρόγραμμα (Main) ---
if __name__ == "__main__":
    # Structures for stats
    delay_stats = []
    queue_size_stats = []
    throughput_stats = []
    loss_rate_stats = []
    
    packets_sent_stats = {'count': 0}
    received_stats = {'count': 0, 'total_bits': 0, 'last_arrival_time': 0}

    print(f"--- Έναρξη Προσομοίωσης (Bandwidth: {BANDWIDTH_BPS/1e6} Mbps, Packets: {NUM_PACKETS}) ---")
    
    env = simpy.Environment()
    
    link_source_to_router = simpy.Store(env)
    link_router_to_destination = simpy.Store(env)
    
    router_node = Router(env, BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK1_S, PROP_DELAY_LINK2_S, QUEUE_SIZE)
    
    # Processes
    env.process(source_host(env, NUM_PACKETS, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_source_to_router, packets_sent_stats))
    env.process(router_node.receive_packets(link_source_to_router))
    env.process(router_node.send_packets(link_router_to_destination))
    env.process(destination_host(env, link_router_to_destination, delay_stats, received_stats))
    
    # Monitors
    # ΔΙΟΡΘΩΣΗ: Προσθήκη του router_node ως ορίσματος
    env.process(monitor_queue_size(env, router_node, 0.01, queue_size_stats))
    env.process(monitor_throughput(env, MONITOR_INTERVAL_S, received_stats, throughput_stats))
    env.process(monitor_packet_loss(env, MONITOR_INTERVAL_S, router_node, packets_sent_stats, loss_rate_stats))
    
    env.run(until=SIM_TIME_S)
    
    print("\n--- Λήξη Προσομοίωσης ---")

    # Συνολικά Αποτελέσματα
    total_packets_sent = packets_sent_stats['count']
    total_packets_dropped = router_node.packets_dropped
    total_packets_received = received_stats['count']
    
    final_packet_loss_rate = (total_packets_dropped / total_packets_sent) * 100 if total_packets_sent > 0 else 0
    
    total_time = received_stats.get('last_arrival_time', 0)
    total_bits_received = received_stats['total_bits']
    # Μέσο throughput συνολικού χρόνου
    average_throughput_bps = total_bits_received / total_time if total_time > 0 else 0

    print("\n--- Τελικά Αποτελέσματα Μετρικών ---")
    print(f"Bandwidth Link: {BANDWIDTH_BPS/1e6} Mbps")
    print(f"Συνολικά πακέτα που στάλθηκαν: {total_packets_sent}")
    print(f"Συνολικά πακέτα που λήφθηκαν: {total_packets_received}")
    print(f"Συνολικά πακέτα που απορρίφθηκαν (Drops): {total_packets_dropped}")
    print(f"Τελικό Ποσοστό Απώλειας (Packet Loss): {final_packet_loss_rate:.2f}%")
    print(f"Μέσο Throughput (συνολικό): {average_throughput_bps / 1_000_000:.4f} Mbps")

    # Plotting
    plt.figure(figsize=(16, 12))

    # 1. Delay
    plt.subplot(2, 2, 1)
    if delay_stats:
        x_val = [item[0] for item in delay_stats]
        y_val = [item[1] for item in delay_stats]
        plt.plot(x_val, y_val, marker='.', linestyle='None', markersize=1, alpha=0.5)
    plt.title("End-to-End Delay Κάθε Πακέτου", fontsize=14)
    plt.xlabel("Χρόνος Άφιξης (s)")
    plt.ylabel("Καθυστέρηση (s)")
    plt.grid(True)

    # 2. Queue Size
    plt.subplot(2, 2, 2)
    if queue_size_stats:
        x_val = [item[0] for item in queue_size_stats]
        y_val = [item[1] for item in queue_size_stats]
        plt.step(x_val, y_val, where='post', color='orange')
        plt.axhline(y=QUEUE_SIZE, color='r', linestyle='--', label='Μέγιστο')
    plt.title("Μέγεθος Ουράς Router", fontsize=14)
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