import simpy

# --- 1. Παράμετροι Προσομοίωσης ---
# Σταθερές για να μπορούμε εύκολα να αλλάξουμε τις συνθήκες του δικτύου.

# Bandwidth σε bits ανά δευτερόλεπτο (bps). 1 Mbps = 1,000,000 bps.
BANDWIDTH_BPS = 1_000_000
# Μετατροπή του bandwidth σε bytes ανά δευτερόλεπτο, αφού τα πακέτα μετριούνται σε bytes.
BANDWIDTH_BYTES_PER_SEC = BANDWIDTH_BPS / 8

PACKET_SIZE_BYTES = 1000  # Μέγεθος κάθε πακέτου σε bytes (π.χ. 1 KB).

# Για να προκαλέσουμε συμφόρηση και packet drops, ορίζουμε τον χρόνο μεταξύ αφίξεων
# μικρότερο από τον χρόνο που απαιτείται για τη μετάδοση ενός πακέτου.
# Χρόνος μετάδοσης = 1000 bytes / 125000 bytes/sec = 0.008 sec.
# Ορίζουμε το διάστημα άφιξης σε 0.005 sec για να γεμίσει η ουρά.
PACKET_INTERVAL_S = 0.005

# Καθυστέρηση διάδοσης για κάθε σύνδεση (σε δευτερόλεπτα).
PROP_DELAY_LINK1_S = 0.050  # 50 ms για τη σύνδεση Source -> Router
PROP_DELAY_LINK2_S = 0.050  # 50 ms για τη σύνδεση Router -> Destination

# ΝΕΑ ΠΑΡΑΜΕΤΡΟΣ: Μέγεθος της ουράς του router.
QUEUE_SIZE = 10

SIM_TIME_S = 0.5  # Αυξήθηκε ο χρόνος για να δούμε περισσότερα γεγονότα.


# --- 2. Μοντέλο Πακέτου ---
# Η κλάση Packet παραμένει η ίδια.
class Packet:
    def __init__(self, time_created, size_bytes, packet_id):
        self.time_created = time_created
        self.size_bytes = size_bytes
        self.packet_id = packet_id

    def __repr__(self):
        return f"Packet(id={self.packet_id}, size={self.size_bytes} B)"


# --- 3. Συμπεριφορά των Κόμβων του Δικτύου ---

def source_host(env, packet_interval, packet_size, link_to_router):
    """
    Αυτή η συνάρτηση μοντελοποιεί τον υπολογιστή-πηγή.
    Δημιουργεί πακέτα σε σταθερά χρονικά διαστήματα.
    """
    packet_id_counter = 0
    while True:
        packet = Packet(
            time_created=env.now,
            size_bytes=packet_size,
            packet_id=packet_id_counter
        )
        print(f"Χρόνος {env.now:.4f}: [Source] Δημιούργησε και στέλνει το Πακέτο {packet.packet_id}")
        link_to_router.put(packet)
        packet_id_counter += 1
        yield env.timeout(packet_interval)


# --- ΝΕΑ ΥΛΟΠΟΙΗΣΗ ΤΟΥ ROUTER ΩΣ ΚΛΑΣΗ ---
class Router:
    def __init__(self, env, bandwidth_bytes_per_sec, prop_delay_s, queue_size):
        self.env = env
        self.bandwidth = bandwidth_bytes_per_sec
        self.prop_delay = prop_delay_s
        # Δημιουργία της ουράς με περιορισμένο μέγεθος (capacity).
        self.queue = simpy.Store(env, capacity=queue_size)
        self.packets_dropped = 0

    def receive_packets(self, link_from_source):
        """
        Διαδικασία που λαμβάνει πακέτα. Αν η ουρά είναι γεμάτη, τα απορρίπτει.
        """
        while True:
            # Αναμονή για λήψη πακέτου από την πηγή.
            packet = yield link_from_source.get()
            
            # Έλεγχος αν η ουρά είναι γεμάτη.
            if len(self.queue.items) < self.queue.capacity:
                # Αν υπάρχει χώρος, το πακέτο μπαίνει στην ουρά.
                print(f"Χρόνος {self.env.now:.4f}: [Router] Έλαβε το Πακέτο {packet.packet_id} και το έβαλε στην ουρά (Μέγεθος ουράς: {len(self.queue.items) + 1}/{self.queue.capacity})")
                self.queue.put(packet)
            else:
                # Αν η ουρά είναι γεμάτη, το πακέτο απορρίπτεται.
                self.packets_dropped += 1
                print(f"!!! Χρόνος {self.env.now:.4f}: [Router] ΑΠΟΡΡΙΨΗ Πακέτου {packet.packet_id} - Η ουρά είναι γεμάτη! (Σύνολο απορρίψεων: {self.packets_dropped})")

    def send_packets(self, link_to_dest):
        """
        Διαδικασία που στέλνει πακέτα από την ουρά προς τον προορισμό.
        """
        while True:
            # Αναμονή μέχρι να υπάρχει ένα πακέτο στην ουρά για αποστολή.
            packet = yield self.queue.get()
            print(f"Χρόνος {self.env.now:.4f}: [Router] Ξεκινά η μετάδοση του Πακέτου {packet.packet_id} από την ουρά.")

            # 1. Υπολογισμός και προσομοίωση της Καθυστέρησης Μετάδοσης (Transmission Delay).
            transmission_delay = packet.size_bytes / self.bandwidth
            yield self.env.timeout(transmission_delay)
            print(f"Χρόνος {self.env.now:.4f}: [Router] Ολοκλήρωσε τη μετάδοση του Πακέτου {packet.packet_id} (delay: {transmission_delay:.4f}s)")

            # 2. Προσομοίωση της Καθυστέρησης Διάδοσης (Propagation Delay).
            yield self.env.timeout(self.prop_delay)
            
            # Προώθηση του πακέτου στην επόμενη σύνδεση.
            link_to_dest.put(packet)


def destination_host(env, link_from_router):
    """
    Μοντελοποιεί τον υπολογιστή-προορισμό. Λαμβάνει πακέτα και υπολογίζει
    τη συνολική καθυστέρηση από άκρο σε άκρο (end-to-end delay).
    """
    packets_received = 0
    total_delay = 0
    while True:
        packet = yield link_from_router.get()
        
        # Η καθυστέρηση διάδοσης της δεύτερης γραμμής έχει ήδη προσομοιωθεί στον router.
        # Οπότε, η τρέχουσα χρονική στιγμή είναι η στιγμή άφιξης.
        
        end_to_end_delay = env.now - packet.time_created
        packets_received += 1
        total_delay += end_to_end_delay
        
        print(f"--------------------------------------------------------------------")
        print(f"Χρόνος {env.now:.4f}: [Destination] Έλαβε το Πακέτο {packet.packet_id}")
        print(f"    >> Συνολική Καθυστέρηση (End-to-End Delay): {end_to_end_delay:.4f} δευτερόλεπτα.")
        print(f"--------------------------------------------------------------------")
    # Σημείωση: Οι παρακάτω γραμμές δεν θα εκτελεστούν ποτέ λόγω του "while True".
    # Για να υπολογίσουμε μετρικές, θα χρειαζόταν ένας πιο σύνθετος μηχανισμός.


# --- 4. Κύριο Μέρος του Προγράμματος ---
if __name__ == "__main__":
    print("--- Έναρξη Προσομοίωσης Δικτύου με Ουρά ---")
    
    env = simpy.Environment()
    
    link_source_to_router = simpy.Store(env)
    link_router_to_destination = simpy.Store(env)
    
    # Δημιουργία του router ως αντικείμενο.
    router_node = Router(env, BANDWIDTH_BYTES_PER_SEC, PROP_DELAY_LINK2_S, QUEUE_SIZE)
    
    # Εκκίνηση των διαδικασιών.
    env.process(source_host(env, PACKET_INTERVAL_S, PACKET_SIZE_BYTES, link_source_to_router))
    # Εκκίνηση των δύο παράλληλων διαδικασιών του router.
    env.process(router_node.receive_packets(link_source_to_router))
    env.process(router_node.send_packets(link_router_to_destination))
    
    env.process(destination_host(env, link_router_to_destination))
    
    env.run(until=SIM_TIME_S)
    
    print("\n--- Λήξη Προσομοίωσης ---")
    print(f"Συνολικά πακέτα που απορρίφθηκαν (Packet Drops): {router_node.packets_dropped}")