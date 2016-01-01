(ns shmoocon
  (:use     [streamparse.specs])
  (:gen-class))

(defn shmoocon [options]
   [
    ;; spout configuration
    {"udp-spout" (python-spout-spec
          options
          "spouts.udpspout.UdpSpout"
          {
          	;;streams to output
          	"orig" ["orig"]
          }
          :conf {
                 "shmoocon.spouts.udpspout.emitStream","orig"
                 }
          )
	}
    ;; bolt configuration
    {	
    "anchor-bolt" (python-bolt-spec
     options
     {["udp-spout" "orig"]["orig"]}
     "bolts.anchor.Anchor"
     {
     "orig" ["orig"]
     }
     :p 1
     :conf {
            "shmoocon.bolts.anchor.emitStream","orig"
            "shmoocon.bolts.anchor.id","uuid"
            }
     )
    }
  ]
)
