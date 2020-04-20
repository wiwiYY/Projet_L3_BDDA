import java.nio.ByteBuffer;

/**
 * 
 * Classe Buffer contenant les frames
 * @author LYZ
 *
 */
public class BufferManager {

	/**
	 * Singleton
	 */
	public Frame[] framePool = new Frame[Constants.FRAME_COUNT];
	
	private BufferManager() {}
	private static BufferManager INSTANCE = null;

	public static BufferManager getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new BufferManager();
		}
		return INSTANCE;
	}

	/**
	 * Recherche le Frame qui correpond a un PageId
	 * 
	 * @param page
	 * @return le Frame qui correspond
	 * @return null si pas de correspondance
	 */
	public Frame searchFrame(PageId pageId) {
		for(Frame f : framePool) {
			if((f!=null) && (f.getPageId()!=null) && (f.getPageId().equals(pageId)))
				return f;
		}
		return null ;
	}

	/**
	 * Cette methode doit reppondre a une demande de page venant des couches plus
	 * hautes, et donc retourner un des buffers associeer a une case. 
	 * 
	 * Le buffer sera rempli avec le contenu de la page designee par argument pageId.
	 * 
	 * @attention : ne pas creer de buffer supplementaire,"recuperer" simplement celui 
	 * qui correspond a la bonne frame, apres l�avoir rempli si besoin par un appel au DiskManager.
	 * @attention : cette methode devra utiuliser une politique de remplacement.
	 * @param pageId un PageId
	 * @return buff un buffer 
	 */
	public ByteBuffer getPage(PageId pageId) {
		ByteBuffer bytebuff ;
		Frame f = searchFrame(pageId);
		if(f==null) {
			int i = indexLibre();
			if(i==-1) {
				i = calcul_LRU(); 
				framePool[i].enregistrerPage();
			}
			framePool[i] = new Frame(pageId);
			framePool[i].chargerPage();
			f = framePool[i];
		}
		bytebuff = f.getBuffer();
		f.incrementePinCount(); 
		return bytebuff ;
	}

	/**
	 * Cette methode devra decrementer le pin_count 
	 * et actualiser le flag dirty de la page pour savoir si elle a ete modifier
	 * On appelle une fonction qui permet de chercher une frame depuis une pageId
	 * Si cette frame existe, on va decrementer son pinCount et mettre a 1 le dirty
	 * Si la pageId n'existe pas, une erreur se produit
	 * 
	 * @param pageId une PageId
	 * @param valdirty un entier booleen
	 */
	public void freePage(PageId pageId, boolean valdirty) {
		Frame f = searchFrame(pageId);
		if (f == null)
			throw new RuntimeException("La frame que l'on cherche n'a pas ete trouve");
		else {
			f.decrementePinCount();
			if(valdirty)
				f.setDirty();
		}
	}
	
	/**
	 * Cette methode permet un entier qui correspond a l'index libre qui correpond au frame libre
	 * @return l'index libre
	 */
	private int indexLibre() {
		for(int i = 0 ; i<framePool.length ; i++) {
			if(framePool[i] == null) {
				return i ;
			}
		}
		return -1 ;
	}

	/**
	 * Algorithme LRU : frame dispo
	 * @return position de la frame dispo
	 */
	public int calcul_LRU() {
		int min = Integer.MAX_VALUE;
		int position = -2;
		for (int i = 0; i < framePool.length; i++) {
			if(framePool[i].getCompteurPersoLRU() < min && framePool[i].getPin_count() == 0) {
				min = framePool[i].getCompteurPersoLRU();
				position = i ;
			}
		}
		if(min == Integer.MAX_VALUE) throw new RuntimeException("Il n'y a pas de frame dispo, car les deux sont en cours d'utilisation");
		return position;
	}
	
	/**
	 * Cette methode s'occupe de : l'ecriture de toutes les pages dont le flag
	 * dirty = 1 sur disque, la remise a 0 de tous les flags/informations et
	 * contenus des buffers (framePool a vide)
	 */
	public void flushAll(){
		for(Frame frame : framePool) {
			/**
			 * Si le frame n'est pas vide et que son pin count est a 1, on l'enregistre sinon on vide juste
			 */
			if(frame != null) {
				frame.enregistrerPage();
				frame.resetFrame();
			}
		}
	}
	
	public Frame[] getFramePool() 	{ return framePool; }

	public void finish() 			{ flushAll();	}

}
