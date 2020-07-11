package com.github.rkredux;

import java.util.Random;

public class VaultApp {

    public static void main(String[] args) {
        Random rand = new Random();
        int password = rand.nextInt(1000);
        Vault vault = new Vault(password);
        Robber robber = new Robber(vault, "RobberOne");
        robber.start();
    }

    private static class Vault {
        private final int password;
        public Vault(int password) {
            this.password = password;
        }
        public static boolean isACorrectPasswordGuess(int guess, int password) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (guess == password) return true;
            return false;
        }
    }

    private static class Robber extends Thread{
        private final Vault vault;
        private final String name;
        public Robber(Vault vault, String name){
            this.vault = vault;
            this.name = name;
        }
        @Override
        public void run(){
            super.run();
            int counter = 1;
            Thread.currentThread().setName(name);
            boolean hasPasswordNotBeenGuessed = true;
            while (hasPasswordNotBeenGuessed){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Random rand = new Random();
                int guess = rand.nextInt(1000);
                if (vault.isACorrectPasswordGuess(guess, vault.password)){
                    System.out.println("Password was guessed correctly by Robber: " + Thread.currentThread().getName() + " In " + counter + " attempts");
                    return;
                };
                System.out.println("Password not guessed correctly in " + counter + " attempt, attempting again");
                counter = counter + 1;
            }
        }
    }

}

