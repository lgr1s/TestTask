#include <WinSock2.h>
#include <WS2tcpip.h>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <vector>

#pragma comment(lib, "ws2_32.lib")

static constexpr int LISTEN_PORT = 3309;
static constexpr int SERVER_PORT = 3306;
static const char* SERVER_HOST = "127.0.0.1";

static constexpr int WORKER_THREAD_COUNT = 4;

static constexpr int BUFFER_SIZE = 4096;

//logging
std::mutex g_logMutex;
void Log(const std::string& msg) {
	std::lock_guard<std::mutex> lk(g_logMutex);
	std::cout << msg << std::endl;
}

enum class IOOperationType {

	Accept,
	ReadClient,
	WriteClient,
	ReadServer,
	WriteServer
};

// OVERLAPPED-context for operation
struct IO_CONTEXT {
	OVERLAPPED overlapped;
	SOCKET socket;
	WSABUF wsabuf;
	char buffer[BUFFER_SIZE];
	IOOperationType operation;
	DWORD TransferredData;
};

struct CONNECTION_CONTEXT {
	SOCKET ClientSocket = INVALID_SOCKET;
	SOCKET ServerSocket = INVALID_SOCKET;

	IO_CONTEXT ClientReadContext;
	IO_CONTEXT ServerReadContext;
	IO_CONTEXT ClientWriteContext;
	IO_CONTEXT ServerWriteContext;

	bool closed = false;

	std::vector<char> clientBuff;
	std::vector<char> serverBuff;
};

//IOCP obj
HANDLE CompletionPort = INVALID_HANDLE_VALUE;

void InitializeContext(IO_CONTEXT& ctx, IOOperationType op, SOCKET s) {

	ctx.operation = op;
	ctx.socket = s;
	ctx.wsabuf.buf = ctx.buffer;
	ctx.wsabuf.len = BUFFER_SIZE;
	ctx.TransferredData = 0;
}

bool SocketToCP(SOCKET s, ULONG_PTR completionKey) {
	HANDLE h = CreateIoCompletionPort((HANDLE)s, CompletionPort, completionKey, 0);
	if (!h)
	{
		Log("CreateIoCompletionPort failed");
		return false;
	}
	return true;
}

bool Send(IO_CONTEXT& ctx, const char* data, int len) {
	DWORD flags = 0;
	DWORD bytesSent = 0;

	ctx.wsabuf.buf = (CHAR*)data;
	ctx.wsabuf.len = len;

	if (WSASend(ctx.socket, &ctx.wsabuf, 1, &bytesSent, flags, &ctx.overlapped, NULL) == SOCKET_ERROR) {
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING && err != 0)
		{
			Log("WSASend failed: " + std::to_string(err));
			return false;
		}
	}
	return true;
}

bool Receive(IO_CONTEXT& ctx) {
	DWORD flags = 0;
	DWORD bytesRecvd = 0;
	ctx.wsabuf.buf = ctx.buffer;
	ctx.wsabuf.len = BUFFER_SIZE;

	if (WSARecv(ctx.socket, &ctx.wsabuf, 1, &bytesRecvd, &flags, &ctx.overlapped, NULL) == SOCKET_ERROR)
	{
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING && err != 0)
		{
			Log("WSARecieve failed: " + std::to_string(err));
			return false;
		}
	}
	return true;
}

void CloseConnection(CONNECTION_CONTEXT* connCont)
{
	if (!connCont->closed) {

		connCont->closed = true;

		shutdown(connCont->ClientSocket, SD_BOTH);
		closesocket(connCont->ClientSocket);
		connCont->ClientSocket = INVALID_SOCKET;
		
		shutdown(connCont->ServerSocket, SD_BOTH);
		closesocket(connCont->ServerSocket);
		connCont->ServerSocket = INVALID_SOCKET;
		
		delete connCont;
	}
}

static const size_t MySqlHeaderSize = 4; // 3 bytes of len, 1 byte of seq
//Parser

void SendToServer(CONNECTION_CONTEXT* connCont, std::vector<char>& packet) {

	IO_CONTEXT& sw = connCont->ServerWriteContext;
	InitializeContext(sw, IOOperationType::WriteServer, connCont->ServerSocket);

	memcpy(sw.buffer, packet.data(), packet.size());
	Send(sw, sw.buffer, static_cast<int>(packet.size()));

}

void ProcessClientBuffer(CONNECTION_CONTEXT* connCont) {

	auto& data = connCont->clientBuff;
	while (true) {
		if (data.size() < MySqlHeaderSize) return;
	}
	unsigned int packetLen = (static_cast<unsigned char>(data[0])) |
		(static_cast<unsigned char>(data[1]) << 8) |
		(static_cast<unsigned char>(data[2]) << 16);

	unsigned int totalLen = MySqlHeaderSize + packetLen;
	if (data.size() < totalLen) {
		return; //Packet is not full. Waiting for the rest
	}

	std::vector<char> packet(data.begin(), data.begin() + totalLen);
	if (packet.size() >= 5) {
		unsigned char command = static_cast<unsigned char>(packet[4]);
		if (command == 0x03) {
			unsigned int sqlLen = packetLen - 1;
			if (sqlLen > 0 && (MySqlHeaderSize + 1 + sqlLen) <= packet.size()) {
				std::string sql(packet.data() + 5, packet.data() + 5 + sqlLen);
			}
		}
	}
}

/*std::string ParseQuery(const char* data, int len) {
	//4th byte - type of request
	//5th byte - request
	if (len < 5) return "";

	unsigned int packetLen = (static_cast<unsigned char>(data[0])) |
		(static_cast<unsigned char>(data[1]) << 8) |
		(static_cast<unsigned char>(data[2]) << 16);

	unsigned char command = static_cast<unsigned char>(data[4]);
	if (command != 0x03) return ""; //COM_QUERY

	int queryLen = packetLen - 1;

	return std::string(data + 5, queryLen);
}*/

void WorkerThread() {

	while (true) {
		DWORD bytesTransferred = 0;
		ULONG_PTR completionKey = 0;
		LPOVERLAPPED overlapped = nullptr;

		bool statusOK = GetQueuedCompletionStatus(CompletionPort, &bytesTransferred, &completionKey, &overlapped, INFINITE);

		CONNECTION_CONTEXT* connCont = reinterpret_cast<CONNECTION_CONTEXT*>(completionKey);
		IO_CONTEXT* IOCont = reinterpret_cast<IO_CONTEXT*>(overlapped); // IO_CONTEXT address correlates with LPOVERLAPPED as Overlapped is the fires field in the IO_CONEXT structure



		IOCont->TransferredData = bytesTransferred;

		switch (IOCont->operation) {

		case IOOperationType::ReadClient: {

			std::string query = ParseQuery(IOCont->buffer, IOCont->TransferredData);
			if (!query.empty()) Log("SQL Query: " + query);

			IO_CONTEXT& servWrite = connCont->ServerWriteContext;
			InitializeContext(servWrite, IOOperationType::WriteServer, connCont->ServerSocket);
			std::memcpy(servWrite.buffer, IOCont->buffer, bytesTransferred);

			if (!Send(servWrite, servWrite.buffer, bytesTransferred)) {
				CloseConnection(connCont);
			}			
			break;
		}
		case IOOperationType::WriteServer: {
			InitializeContext(connCont->ServerReadContext, IOOperationType::ReadServer, connCont->ServerSocket);
			if (!Receive(connCont->ServerReadContext)) {
				CloseConnection(connCont);
			}
			break;
		}
		case IOOperationType::ReadServer: {

			//resend server response to client
			IO_CONTEXT& clientWrite = connCont->ClientWriteContext;
			InitializeContext(clientWrite, IOOperationType::WriteClient, connCont->ClientSocket);
			std::memcpy(clientWrite.buffer, IOCont->buffer, bytesTransferred);

			if (!Send(clientWrite, clientWrite.buffer, bytesTransferred)) {
				CloseConnection(connCont);
			}
			InitializeContext(connCont->ServerReadContext, IOOperationType::ReadServer, connCont->ServerSocket);
			Receive(connCont->ServerReadContext);
			break;
		}
		case IOOperationType::WriteClient: {
			InitializeContext(connCont->ClientReadContext, IOOperationType::ReadClient, connCont->ClientSocket);
			if (!Receive(connCont->ClientReadContext)) {
				CloseConnection(connCont);
			}
			break;
		}

		default:
			break;
		}
	}
}

SOCKET ServerConnection(const char* host, int port) {

	SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (s == INVALID_SOCKET) {
		Log("Socket creation failed");
		return INVALID_SOCKET;
	}

	sockaddr_in serverAddr;
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_port = htons(port);
	inet_pton(AF_INET, host, &serverAddr.sin_addr);
	if (connect(s, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
		Log("connection failed");
		closesocket(s);
		return INVALID_SOCKET;
	}
	return s;
}


bool StartProxyServer() {

	CompletionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, WORKER_THREAD_COUNT);
	if (!CompletionPort) {
		Log("Cretion of IOCP has failed");
		return false;
	}

	for (int i = 0; i < WORKER_THREAD_COUNT; ++i) {
		std::thread(WorkerThread).detach();
	}

	SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (listenSock == INVALID_SOCKET) {
		Log("Socket has failed!");
		return false;
	}

	sockaddr_in localAddr;
	localAddr.sin_family = AF_INET;
	localAddr.sin_addr.s_addr = htonl(INADDR_ANY);
	localAddr.sin_port = htons(LISTEN_PORT);

	if (bind(listenSock, (sockaddr*)&localAddr, sizeof(localAddr)) == SOCKET_ERROR) {
		Log("Bind has failed!");
		closesocket(listenSock);
		return false;
	}
	if (listen(listenSock, SOMAXCONN) == SOCKET_ERROR) {
		Log("Listen has failed!");
		return false;
	}
	Log("Proxy listening on port " + std::to_string(LISTEN_PORT));

	while (true) {
		sockaddr_in clientAddr;
		int addrLen = sizeof(clientAddr);
		SOCKET clientSock = accept(listenSock, (sockaddr*)&clientAddr, &addrLen);
		if (clientSock == INVALID_SOCKET)
		{
			int error = WSAGetLastError();
			if (error == WSAEWOULDBLOCK)
			{
				Sleep(50);
				continue;
			}
			else
			{
				Log("accept() failed: " + std::to_string(error));
				continue;
			}
		}
		SOCKET serverSock = ServerConnection(SERVER_HOST, SERVER_PORT);
		if (serverSock == INVALID_SOCKET)
		{
			Log("Server Connection has failed!");
			closesocket(clientSock);
			continue;
		}
		CONNECTION_CONTEXT* connCont = new CONNECTION_CONTEXT();
		connCont->ClientSocket = clientSock;
		connCont->ServerSocket = serverSock;

		if (!SocketToCP(clientSock, (ULONG_PTR)connCont) || !SocketToCP(serverSock, (ULONG_PTR)connCont)) {
			Log("Associating socket with IOCP has faied!");
			CloseConnection(connCont);
		}

		InitializeContext(connCont->ClientReadContext, IOOperationType::ReadClient, clientSock);
		InitializeContext(connCont->ClientWriteContext, IOOperationType::WriteClient, clientSock);
		InitializeContext(connCont->ServerReadContext, IOOperationType::ReadServer, serverSock);
		InitializeContext(connCont->ServerWriteContext, IOOperationType::WriteServer, serverSock);

		if (!Receive(connCont->ClientReadContext)) {
			CloseConnection(connCont);
			continue;
		}
		if (!Receive(connCont->ServerReadContext)) {
			CloseConnection(connCont);
			continue;
		}

		//log connection
		char ip[INET_ADDRSTRLEN] = { 0 };
		inet_ntop(AF_INET, &clientAddr.sin_addr, ip, sizeof(ip));
		Log(std::string("Client connection: ") + ip);

	}
	closesocket(listenSock);
	return true;
}

int main() {
	WSADATA wsaData;
	int r = WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (r != 0)
	{
		Log("WSAStartup failed: " + std::to_string(r));
		return 1;
	}
	StartProxyServer();
	WSACleanup();
	return 0;
}