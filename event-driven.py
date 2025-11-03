import simpy
import matplotlib.pyplot as plt

# --- 1. Παράμετροι Προσομοίωσης ---
# Σταθερές για να μπορούμε εύκολα να αλλάξουμε τις συνθήκες του δικτύου.

# Bandwidth σε bits ανά δευτερόλεπτο (bps). 1 Mbps = 1,000,000 bps.
BANDWIDTH_BPS = 1_000_000
# Μετατροπή του bandwidth σε bytes ανά δευτερόλεπτο, αφού τα πακέτα μετριούνται σε bytes.
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8

PACKET_SIZE_BYTES = 1000  # Μέγεθος κάθε πακέτου σε bytes (π.χ. 1 KB).

# Ο ρυθμός άφιξης είναι 1 πακέτο / 0.005s = 200 πακέτα/δευτερόλεπτο.
PACKET_INTERVAL_S = 0.005

# Καθυστέρηση διάδοσης για κάθε σύνδεση (σε δευτερόλεπτα).
PROP_DELAY_LINK1_S = 0.050  # 50 ms για τη σύνδεση Source -> Router
PROP_DELAY_LINK2_S = 0.050  # 50 ms για τη σύνδεση Router -> Destination

# Μέγεθος της ουράς του router.
QUEUE_SIZE = 10

# --- Παράμετροι για το Ερώτημα 4 ---
NUM_PACKETS = 200  # Αριθμός πακέτων προς αποστολή

# Αυξημένος χρόνος προσομοίωσης για να προλάβει το σύστημα να επεξεργαστεί τα πακέτα
# και να φανεί το πραγματικό αποτέλεσμα της συμφόρησης.
SIM_TIME_S = 15.0

QUEUE_MONITOR_INTERVAL_S = 0.001 # Ρυθμός δειγματοληψίας για το μέγεθος της ουράς


# --- 2. Μοντέλο Πακέτου ---
class Packet:
    def __init__(self, time_created, size_bytes, packet_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id

    def __repr__(self):
        return f"Packet(id={self.packet_id}, size={self.size_bytes} B)"


# --- 3. Συμπεριφορά των Κόμβων του Δικτύου ---

def source_host(env, num_packets, packet_interval, packet_size, link_to_router, packets_sent_stats):
    """
    Αυτή η συνάρτηση μοντελοποιεί τον υπολογιστή-πηγή.
    """
    for packet_id_counter in range(num_packets):
        packet = Packet(
            time_created=env.now,
            size_bytes=packet_size,
            packet_id=packet_id_counter
        )
        print(f"Χρόνος {env.now:.4f}: [Source] Δημιούργησε και στέλνει το Πακέτο {packet.packet_id}")
        link_to_router.put(packet)
        packets_sent_stats['count'] += 1
        yield env.timeout(packet_interval)


class Router:
    """
    ### ΤΕΛΙΚΗ ΣΩΣΤΗ ΥΛΟΠΟΙΗΣΗ ###
    Αυτή η κλάση Router μοντελοποιεί σωστά την παράλληλη άφιξη πακέτων.
    Η `receive_packets` δεν μπλοκάρει, αλλά ξεκινά μια νέα διαδικασία `handle_arrival`
    για κάθε πακέτο, επιτρέποντας τη σωστή προσομοίωση της συμφόρησης.
    """
    def __init__(self, env, bandwidth_bytes_per_sec, prop_delay_link1_s, prop_delay_link2_s, queue_size):
        self.env = env
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay_link1 = prop_delay_link1_s
        self.prop_delay_link2 = prop_delay_link2_s
        self.queue = simpy.Store(env, capacity=queue_size)
        self.packets_dropped = 0

    def receive_packets(self, link_from_source):
        """
        Λαμβάνει πακέτα και ξεκινά μια νέα, παράλληλη διαδικασία για το καθένα.
        """
        while True:
            packet = yield link_from_source.get()
            self.env.process(self.handle_arrival(packet))

    def handle_arrival(self, packet):
        """
        Προσομοιώνει την καθυστέρηση διάδοσης για ΕΝΑ πακέτο και μετά το βάζει στην ουρά.
        """
        yield self.env.timeout(self.prop_delay_link1)
        
        if len(self.queue.items) < self.queue.capacity:
            print(f"Χρόνος {self.env.now:.4f}: [Router] Έλαβε το Πακέτο {packet.packet_id} και το έβαλε στην ουρά (Μέγεθος ουράς: {len(self.queue.items) + 1}/{self.queue.capacity})")
            self.queue.put(packet)
        else:
            self.packets_dropped += 1
            print(f"!!! Χρόνος {self.env.now:.4f}: [Router] ΑΠΟΡΡΙΨΗ Πακέτου {packet.packet_id} - Η ουρά είναι γεμάτη! (Σύνολο απορρίψεων: {self.packets_dropped})")

    def send_packets(self, link_to_dest):
        """
        Εξυπηρετεί την ουρά, στέλνοντας πακέτα προς τον προορισμό.
        """
        while True:
            packet = yield self.queue.get()
            print(f"Χρόνος {self.env.now:.4f}: [Router] Ξεκινά η μετάδοση του Πακέτου {packet.packet_id} από την ουρά.")

            transmission_delay = packet.size_bytes / self.bandwidth
            yield self.env.timeout(transmission_delay)
            print(f"Χρόνος {self.env.now:.4f}: [Router] Ολοκλήρωσε τη μετάδοση του Πακέτου {packet.packet_id} (delay: {transmission_delay:.4f}s)")

            yield self.env.timeout(self.prop_delay_link2)
            
            link_to_dest.put(packet)


def destination_host(env, link_from_router, delay_stats, received_stats):
    """
    Μοντελοποιεί τον υπολογιστή-προορισμό.
    """
    while True:
        packet = yield link_from_router.get()
        
        arrival_time = env.now
        end_to_end_delay = arrival_time - packet.time_created
        
        delay_stats.append((arrival_time, end_to_end_delay))
        received_stats['count'] += 1
        received_stats['total_bits'] += packet.size_bytes * 8
        received_stats['last_arrival_time'] = arrival_time
        
        print(f"--------------------------------------------------------------------")
        print(f"Χρόνος {arrival_time:.4f}: [Destination] Έλαβε το Πακέτο {packet.packet_id}")
        print(f"    >> Συνολική Καθυστέρηση (End-to-End Delay): {end_to_end_delay:.4f} δευτερόλεπτα.")
        print(f"--------------------------------------------------------------------")


def monitor_queue_size(env, router, interval, queue_size_stats):
    """
    Παρακολουθεί το μέγεθος της ουράς του router.
    """
    while True:
        current_queue_size = len(router.queue.items)
        queue_size_stats.append((env.now, current_queue_size))
        yield env.timeout(interval)


# --- 4. Κύριο Μέρος του Προγράμματος ---
if __name__ == "__main__":
    delay_stats = []
    queue_size_stats = []
    packets_sent_stats = {'count': 0}
    received_stats = {'count': 0, 'total_bits': 0, 'last_arrival_time': 0}

    print("--- Έναρξη Προσομοίωσης Δικτύου με Ουρά ---")
    
    env = simpy.Environment()
    
    link_source_to_router = simpy.Store(env)
    link_router_to_destination = simpy.Store(env)
    
    router_node = Router(env, BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK1_S, PROP_DELAY_LINK2_S, QUEUE_SIZE)
    
    env.process(source_host(env, NUM_PACKETS, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_source_to_router, packets_sent_stats))
    env.process(router_node.receive_packets(link_source_to_router))
    env.process(router_node.send_packets(link_router_to_destination))
    env.process(destination_host(env, link_router_to_destination, delay_stats, received_stats))
    env.process(monitor_queue_size(env, router_node, QUEUE_MONITOR_INTERVAL_S, queue_size_stats))
    
    env.run(until=SIM_TIME_S)
    
    print("\n--- Λήξη Προσομοίωσης ---")

    total_packets_sent = packets_sent_stats['count']
    total_packets_dropped = router_node.packets_dropped
    total_packets_received = received_stats['count']
    
    packet_loss_rate = (total_packets_dropped / total_packets_sent) * 100 if total_packets_sent > 0 else 0
    
    total_time = received_stats.get('last_arrival_time', 0)
    total_bits_received = received_stats['total_bits']
    throughput_bps = total_bits_received / total_time if total_time > 0 else 0

    print("\n--- Αποτελέσματα Μετρικών ---")
    print(f"Συνολικά πακέτα που στάλθηκαν: {total_packets_sent}")
    print(f"Συνολικά πακέτα που λήφθηκαν: {total_packets_received}")
    print(f"Συνολικά πακέτα που απορρίφθηκαν (Packet Drops): {total_packets_dropped}")
    print(f"Ποσοστό Απώλειας Πακέτων (Packet Loss Rate): {packet_loss_rate:.2f}%")
    print(f"Throughput: {throughput_bps / 1_000_000:.4f} Mbps")

    plt.figure(figsize=(14, 12))

    plt.subplot(2, 1, 1)
    if delay_stats:
        arrival_times = [item[0] for item in delay_stats]
        delays = [item[1] for item in delay_stats]
        plt.plot(arrival_times, delays, marker='.', linestyle='-', markersize=4)
    plt.title("End-to-End Delay Κάθε Πακέτου", fontsize=16)
    plt.xlabel("Χρόνος Άφιξης (s)", fontsize=12)
    plt.ylabel("Καθυστέρηση (s)", fontsize=12)
    plt.grid(True)

    plt.subplot(2, 1, 2)
    if queue_size_stats:
        time_points = [item[0] for item in queue_size_stats]
        q_sizes = [item[1] for item in queue_size_stats]
        plt.plot(time_points, q_sizes, drawstyle='steps-post', color='orange')
        plt.axhline(y=QUEUE_SIZE, color='r', linestyle='--', label=f'Μέγιστο Μέγεθος Ουράς ({QUEUE_SIZE})')
    plt.title("Μέγεθος Ουράς Router σε Σχέση με τον Χρόνο", fontsize=16)
    plt.xlabel("Χρόνος (s)", fontsize=12)
    plt.ylabel("Πακέτα στην Ουρά", fontsize=12)
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout(pad=3.0)
    plt.show()