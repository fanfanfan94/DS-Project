import java.net.*;
import java.io.*;
import java.util.*;

class GlobalExchange implements Runnable{
	private HashMap<String, Integer> localExchanges;
	private HashMap<String, Integer> globalExchanges;
	private HashMap<String, Integer> localStocks;
	private ServerSocket serverSocket;
	private String location;
	private Socket server;

	public GlobalExchange(int port, String location) throws IOException {
		this.location = location;
		serverSocket = new ServerSocket(port);
		localExchanges = new HashMap<String, Integer>();
		globalExchanges = new HashMap<String, Integer>();
		localStocks = new HashMap<String, Integer>();

		if (location.equals("ASIA")) {
			globalExchanges.put("EUROPE", 6667);
			globalExchanges.put("AMERICA", 6668);
			globalExchanges.put("AFRICA", 6669);
		} else if (location.equals("EUROPE")) {
			globalExchanges.put("ASIA", 6666);
			globalExchanges.put("AMERICA", 6668);
			globalExchanges.put("AFRICA", 6669);
		} else if (location.equals("AMERICA")) {
			globalExchanges.put("EUROPE", 6667);
			globalExchanges.put("ASIA", 6666);
			globalExchanges.put("AFRICA", 6669);
		} else if (location.equals("AFRICA")) {
			globalExchanges.put("EUROPE", 6667);
			globalExchanges.put("AMERICA", 6668);
			globalExchanges.put("ASIA", 6666);
		}
	}

	public void run() {
		while (true) {
			ProcessCommands();
		}
	}

	public void ProcessCommands() {
		try {
			server = serverSocket.accept();
			System.out.println("Connected to " + server.getRemoteSocketAddress());
			DataInputStream in = new DataInputStream(server.getInputStream());
			String commands = in.readUTF();
			System.out.println(commands);
			String[] command = commands.split(",");
			if (command[0].equals("Register")) {
				System.out.println("Registering " + command[1] + " " + command[2]);
				localExchanges.put(command[1], Integer.parseInt(command[2]));
				List<String> stks = Arrays.asList(command);
				for (int i = 3; i < stks.size(); i++) {
					localStocks.put(stks.get(i), Integer.parseInt(command[2]));
				}
			} else if (command[0].equals("Query")) {
				System.out.println("Querying," + command[1]);
				OutputStream outToPeer = server.getOutputStream();
				DataOutputStream dataOutToPeer = new DataOutputStream(outToPeer);
				if (!localExchanges.containsKey(command[1])) {
					dataOutToPeer.writeUTF("NOT FOUND");
				} else {
					Integer destPort = localExchanges.get(command[1]);
					String stockName = command[1];
					int number = Integer.parseInt(command[2]);
					String res = BuyFromExchange(destPort, command[1], number);
					dataOutToPeer.writeUTF(res);
				}
			} else if (command[0].equals("Buy")) {
				if (localStocks.containsKey(command[1])) {
					int destPort = localStocks.get(command[1]);
					int number = Integer.parseInt(command[2]);
					String res = BuyFromExchange(destPort, command[1], number);
					OutputStream outToClient = server.getOutputStream();
					DataOutputStream dataOutToServer = new DataOutputStream(outToClient);
					dataOutToServer.writeUTF(res);
				} else {
					String res = QueryPeer(command[1], Integer.parseInt(command[2]));	
					OutputStream outToClient = server.getOutputStream();
					DataOutputStream dataOutToServer = new DataOutputStream(outToClient);
					dataOutToServer.writeUTF(res);
				}	
			} else if (command[0].equals("Delete")) {
				System.out.println("Deleting Local Exchange " + command[1]);
				localExchanges.remove(command[1]);
				OutputStream outToClient = server.getOutputStream();
				DataOutputStream dataOutToServer = new DataOutputStream(outToClient);
				dataOutToServer.writeUTF("Local Exchange Deleted");
			}
		}catch(SocketTimeoutException s) {
			System.out.println("Socket timed out!");
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	public String BuyFromExchange(int dest, String stockName, int number) {
		String res = "";
		try {
			Socket peer = new Socket("localhost", dest);
			OutputStream outToLocalExchange = peer.getOutputStream();
			DataOutputStream dataOutToExchange = new DataOutputStream(outToLocalExchange);
			dataOutToExchange.writeUTF("BUYG," + stockName + " " + String.valueOf(number));

			InputStream inFromPeer = peer.getInputStream();
			DataInputStream infromPeer = new DataInputStream(inFromPeer);
			String resFromPeer = infromPeer.readUTF();
			System.out.println("Result of Query: " + resFromPeer);

			if (resFromPeer.equals("false")) {
				res = "BUYING FAILED";
			} else {
				res = "BUYING SUCCESS";
			}
		} catch(SocketTimeoutException s) {
			System.out.println("Socket timed out!");
		} catch(IOException e) {
			e.printStackTrace();
		}
		return res;
	}	

	public String QueryPeer(String stockName, int number) {
		try {
			List<Integer> peerPorts = new ArrayList<Integer>(globalExchanges.values());
			for (int i = 0; i < peerPorts.size(); i++) {
				Socket peer = new Socket("localhost", peerPorts.get(i));
				OutputStream outToLocalExchange = peer.getOutputStream();
				DataOutputStream dataOutToExchange = new DataOutputStream(outToLocalExchange);
				dataOutToExchange.writeUTF("Query " + stockName + " " + String.valueOf(number));

				InputStream inFromPeer = peer.getInputStream();
				DataInputStream infromPeer = new DataInputStream(inFromPeer);
				String destFromPeer = infromPeer.readUTF();
				System.out.println("Result of Query: " + destFromPeer);	

				if (destFromPeer.equals("NOT FOUND")) continue;
				else {
					return destFromPeer;
				}
			}
		} catch(SocketTimeoutException s) {
			System.out.println("Socket timed out!");
		} catch(IOException e) {
			e.printStackTrace();
		}
		return "NOT FOUND";
	}

	public static void main(String [] args) throws IOException {
		String location = args[0]; 
		Integer port = 0;
		if (location.equals("ASIA")) {
			port = 6666;
		} else if (location.equals("EUROPE")) {
			port = 6667;
		} else if (location.equals("AMERICA")) {
			port = 6668;
		} else if (location.equals("AFRICA")) {
			port = 6669;
		}
		try {
			new Thread(new GlobalExchange(port, location)).start();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
}

//GlobalExchange首先获取localexchange的请求，然后先从本地其他localexchange读数据，如果没有的话就从其他globalexchange请求